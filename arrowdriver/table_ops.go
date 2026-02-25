package arrowdriver

import (
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/grip/log"
	"go.etcd.io/bbolt"
)

func (t *ArrowTable) GetColumnDefs() []benchtop.ColumnDef {
	out := make([]benchtop.ColumnDef, len(t.columns))
	copy(out, t.columns)
	return out
}

func (t *ArrowTable) HasField(field string) bool {
	if field == idColumn {
		return true
	}
	for _, c := range t.columns {
		if c.Key == field {
			return true
		}
	}
	return true
}

func (t *ArrowTable) AddRow(elem benchtop.Row) (*benchtop.RowLoc, error) {
	locs, err := t.AddRows([]benchtop.Row{elem})
	if err != nil {
		return nil, err
	}
	if len(locs) == 0 {
		return nil, fmt.Errorf("no location returned")
	}
	return locs[0], nil
}

func (t *ArrowTable) AddRows(elems []benchtop.Row) ([]*benchtop.RowLoc, error) {
	if len(elems) == 0 {
		return []*benchtop.RowLoc{}, nil
	}
	start := time.Now()
	t.lock.Lock()
	defer t.lock.Unlock()

	section, err := t.reserveSection()
	if err != nil {
		return nil, err
	}
	sectionRows, err := buildSectionRows(elems)
	if err != nil {
		return nil, err
	}
	if err := t.writeSectionMaterialized(section, sectionRows); err != nil {
		return nil, err
	}
	indexableRows := make([]map[string]any, len(elems))
	seenFields := map[string]struct{}{}
	for i := range sectionRows {
		vals := map[string]any{}
		for f, v := range sectionRows[i].cols {
			if !isIndexableValue(v) {
				continue
			}
			vals[f] = v
		}
		indexableRows[i] = vals
		for f := range vals {
			seenFields[f] = struct{}{}
		}
	}
	newFields := false
	for f := range seenFields {
		if _, ok := t.indexedFields[f]; !ok {
			newFields = true
		}
		t.indexedFields[f] = struct{}{}
	}

	locs := make([]*benchtop.RowLoc, len(elems))
	err = t.indexDB.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(idsBucket))
		fwd := tx.Bucket([]byte(fieldIndexBucket))
		rev := tx.Bucket([]byte(reverseFieldIndexBucket))
		if b == nil {
			return fmt.Errorf("missing ids bucket")
		}
		if newFields {
			if err := t.persistIndexedFieldsLocked(tx); err != nil {
				return err
			}
		}
		for i, row := range elems {
			loc := &benchtop.RowLoc{TableId: t.tableID, Section: section, Offset: uint32(i), Size: 0, Index: 0}
			if err := b.Put(row.Id, encodeRowLoc(loc)); err != nil {
				return err
			}
			if fwd != nil && rev != nil {
				rowID := string(row.Id)
				for field, fieldVal := range indexableRows[i] {
					valueBytes, ok := encodeIndexValue(fieldVal)
					if !ok {
						continue
					}
					if err := fwd.Put(makeFieldIndexKey(field, valueBytes, rowID), encodeRowLoc(loc)); err != nil {
						return err
					}
					if err := rev.Put(makeReverseFieldIndexKey(field, rowID), valueBytes); err != nil {
						return err
					}
				}
			}
			locs[i] = loc
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	t.invalidateExecutionCaches()

	log.Debugf("arrowtable.AddRows table=%s tableID=%d section=%d rows=%d elapsed=%s", t.name, t.tableID, section, len(elems), time.Since(start).Round(time.Millisecond))
	return locs, nil
}

func (t *ArrowTable) DeleteRow(loc *benchtop.RowLoc, id []byte) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if len(id) == 0 {
		return nil
	}
	idStr := string(id)
	err := t.indexDB.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(idsBucket))
		fwd := tx.Bucket([]byte(fieldIndexBucket))
		rev := tx.Bucket([]byte(reverseFieldIndexBucket))
		if b == nil {
			return fmt.Errorf("missing ids bucket")
		}
		if fwd != nil && rev != nil {
			for field := range t.indexedFields {
				rKey := makeReverseFieldIndexKey(field, idStr)
				val := rev.Get(rKey)
				if val == nil {
					continue
				}
				if err := fwd.Delete(makeFieldIndexKey(field, val, idStr)); err != nil {
					return err
				}
				if err := rev.Delete(rKey); err != nil {
					return err
				}
			}
		}
		return b.Delete(id)
	})
	if err == nil {
		t.invalidateExecutionCaches()
	}
	return err
}

func (t *ArrowTable) MarkDeleteTable(loc *benchtop.RowLoc) error {
	return nil
}

func (t *ArrowTable) GetRowLoc(id string) (*benchtop.RowLoc, error) {
	var loc *benchtop.RowLoc
	err := t.indexDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(idsBucket))
		if b == nil {
			return fmt.Errorf("missing ids bucket")
		}
		v := b.Get([]byte(id))
		if v == nil {
			return fmt.Errorf("id %s not found", id)
		}
		decoded, err := decodeRowLoc(v)
		if err != nil {
			return err
		}
		loc = decoded
		return nil
	})
	return loc, err
}

func (t *ArrowTable) GetRow(loc *benchtop.RowLoc) (map[string]any, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	if cached, ok := t.getCachedSectionRows(loc.Section); ok {
		if int(loc.Offset) < len(cached) {
			return cloneRowMap(cached[loc.Offset]), nil
		}
	}
	rows, _, err := t.readSectionRows(loc.Section)
	if err != nil {
		return nil, err
	}
	if int(loc.Offset) >= len(rows) {
		return nil, fmt.Errorf("row not found at section=%d offset=%d", loc.Section, loc.Offset)
	}
	return cloneRowMap(rows[loc.Offset]), nil
}

func (t *ArrowTable) GetRows(locs []*benchtop.RowLoc) ([]map[string]any, []error) {
	results := make([]map[string]any, len(locs))
	errs := make([]error, len(locs))

	bySection := map[uint16][]int{}
	for i, loc := range locs {
		if loc == nil {
			errs[i] = fmt.Errorf("nil row location")
			continue
		}
		bySection[loc.Section] = append(bySection[loc.Section], i)
	}

	type sectionWork struct {
		sec  uint16
		idxs []int
	}
	workCh := make(chan sectionWork, len(bySection))
	var wg sync.WaitGroup
	workers := runtime.NumCPU()
	if workers < 2 {
		workers = 2
	}
	if workers > 16 {
		workers = 16
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range workCh {
				offsets := map[uint32]struct{}{}
				for _, i := range work.idxs {
					offsets[locs[i].Offset] = struct{}{}
				}
				rowsByOffset, err := t.readSectionRowsByOffsets(work.sec, offsets)
				if err != nil {
					for _, i := range work.idxs {
						errs[i] = err
					}
					continue
				}
				for _, i := range work.idxs {
					loc := locs[i]
					row, ok := rowsByOffset[loc.Offset]
					if !ok {
						errs[i] = fmt.Errorf("row not found at section=%d offset=%d", loc.Section, loc.Offset)
						continue
					}
					results[i] = row
				}
			}
		}()
	}
	for sec, idxs := range bySection {
		workCh <- sectionWork{sec: sec, idxs: idxs}
	}
	close(workCh)
	wg.Wait()

	return results, errs
}

func (t *ArrowTable) ListTableKeys() (chan benchtop.Index, error) {
	out := make(chan benchtop.Index, 100)
	go func() {
		defer close(out)
		rows, err := t.listIndexRows()
		if err != nil {
			return
		}
		for _, r := range rows {
			out <- benchtop.Index{Key: []byte(r.id), Loc: r.loc}
		}
	}()
	return out, nil
}

func (t *ArrowTable) TableID() uint16 {
	return t.tableID
}

// CompactSections rewrites scattered small section files into larger sections.
// This is intended to run after bulk load to reduce file count and scan overhead.
func (t *ArrowTable) CompactSections(maxRowsPerSection int) error {
	if maxRowsPerSection <= 0 {
		maxRowsPerSection = defaultCompactRowsPerSection
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	indexed, err := t.listIndexRows()
	if err != nil {
		return err
	}
	if len(indexed) <= maxRowsPerSection {
		return nil
	}

	oldSectionsMap := map[uint16]struct{}{}
	for _, r := range indexed {
		oldSectionsMap[r.loc.Section] = struct{}{}
	}
	if len(oldSectionsMap) <= 1 {
		return nil
	}

	type stagedRow struct {
		id   string
		data map[string]any
	}

	staged := make([]stagedRow, 0, maxRowsPerSection)
	var currSection uint16 = ^uint16(0)
	var secRows []map[string]any

	flush := func() error {
		if len(staged) == 0 {
			return nil
		}

		section, err := t.reserveSection()
		if err != nil {
			return err
		}

		rows := make([]benchtop.Row, len(staged))
		for i, s := range staged {
			clean := make(map[string]any, len(s.data))
			for k, v := range s.data {
				if k == idColumn {
					continue
				}
				clean[k] = v
			}
			rows[i] = benchtop.Row{Id: []byte(s.id), Data: clean}
		}

		if err := t.writeSection(section, rows); err != nil {
			return err
		}

		err = t.indexDB.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(idsBucket))
			if b == nil {
				return fmt.Errorf("missing ids bucket")
			}
			for i, s := range staged {
				loc := &benchtop.RowLoc{TableId: t.tableID, Section: section, Offset: uint32(i), Size: 0, Index: 0}
				if err := b.Put([]byte(s.id), encodeRowLoc(loc)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}

		staged = staged[:0]
		return nil
	}

	for _, r := range indexed {
		if r.loc.Section != currSection {
			secRows, _, err = t.readSectionRows(r.loc.Section)
			if err != nil {
				return err
			}
			currSection = r.loc.Section
		}
		if int(r.loc.Offset) >= len(secRows) {
			return fmt.Errorf("row not found at section=%d offset=%d", r.loc.Section, r.loc.Offset)
		}
		staged = append(staged, stagedRow{id: r.id, data: secRows[int(r.loc.Offset)]})
		if len(staged) >= maxRowsPerSection {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}

	for sec := range oldSectionsMap {
		t.invalidateSectionCaches(sec)
		_ = os.Remove(t.sectionPath(sec))
	}
	t.clearAllSectionCaches()
	t.invalidateExecutionCaches()

	return nil
}

func (t *ArrowTable) SetTableID(id uint16) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.tableID = id
	return t.indexDB.Update(func(tx *bbolt.Tx) error {
		mb := tx.Bucket([]byte(metaBucket))
		if mb == nil {
			return fmt.Errorf("missing meta bucket")
		}
		v := make([]byte, 2)
		binary.LittleEndian.PutUint16(v, id)
		return mb.Put([]byte(metaTableIDKey), v)
	})
}
