package arrowdriver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/tpath"
	"github.com/bytedance/sonic"
	"go.etcd.io/bbolt"
)

func makeFieldIndexKey(field string, valueBytes []byte, rowID string) []byte {
	return bytes.Join([][]byte{[]byte(field), valueBytes, []byte(rowID)}, []byte{0x1f})
}

func makeFieldIndexPrefix(field string, valueBytes []byte) []byte {
	return bytes.Join([][]byte{[]byte(field), valueBytes}, []byte{0x1f})
}

func encodeIndexValue(v any) ([]byte, bool) {
	switch x := v.(type) {
	case string:
		out := make([]byte, 1+len(x))
		out[0] = 's'
		copy(out[1:], x)
		return out, true
	case bool:
		out := []byte{'b', 0}
		if x {
			out[1] = 1
		}
		return out, true
	}
	if fv, ok := toFloat64(v); ok {
		out := make([]byte, 1+8)
		out[0] = 'f'
		binary.LittleEndian.PutUint64(out[1:], math.Float64bits(fv))
		return out, true
	}
	valueBytes, err := sonic.ConfigFastest.Marshal(v)
	if err != nil {
		return nil, false
	}
	out := make([]byte, 1+len(valueBytes))
	out[0] = 'j'
	copy(out[1:], valueBytes)
	return out, true
}

func makeReverseFieldIndexKey(field, rowID string) []byte {
	return bytes.Join([][]byte{[]byte(field), []byte(rowID)}, []byte{0x1f})
}

func indexedIDFromKey(key []byte) (string, bool) {
	i := bytes.LastIndexByte(key, 0x1f)
	if i < 0 || i+1 >= len(key) {
		return "", false
	}
	return string(key[i+1:]), true
}

func (t *ArrowTable) persistIndexedFieldsLocked(tx *bbolt.Tx) error {
	mb := tx.Bucket([]byte(metaBucket))
	if mb == nil {
		return fmt.Errorf("missing meta bucket")
	}
	idxFields := make([]string, 0, len(t.indexedFields))
	for f := range t.indexedFields {
		idxFields = append(idxFields, f)
	}
	sort.Strings(idxFields)
	b, err := sonic.ConfigFastest.Marshal(idxFields)
	if err != nil {
		return err
	}
	return mb.Put([]byte(metaIndexKey), b)
}

func (t *ArrowTable) removeFieldIndexLocked(tx *bbolt.Tx, field string) error {
	fwd := tx.Bucket([]byte(fieldIndexBucket))
	rev := tx.Bucket([]byte(reverseFieldIndexBucket))
	if fwd == nil || rev == nil {
		return fmt.Errorf("missing field index buckets")
	}
	prefix := append([]byte(field), 0x1f)
	for c, k := fwd.Cursor(), []byte(nil); ; {
		if k == nil {
			k, _ = c.Seek(prefix)
		} else {
			k, _ = c.Next()
		}
		if k == nil || !bytes.HasPrefix(k, prefix) {
			break
		}
		if err := c.Delete(); err != nil {
			return err
		}
	}
	for c, k := rev.Cursor(), []byte(nil); ; {
		if k == nil {
			k, _ = c.Seek(prefix)
		} else {
			k, _ = c.Next()
		}
		if k == nil || !bytes.HasPrefix(k, prefix) {
			break
		}
		if err := c.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func (t *ArrowTable) buildFieldIndexLocked(field string) error {
	rows, err := t.listIndexRows()
	if err != nil {
		return err
	}
	bySection := map[uint16][]indexedRow{}
	for _, r := range rows {
		bySection[r.loc.Section] = append(bySection[r.loc.Section], r)
	}
	sections := make([]int, 0, len(bySection))
	for sec := range bySection {
		sections = append(sections, int(sec))
	}
	sort.Ints(sections)

	for _, secInt := range sections {
		sec := uint16(secInt)
		secRows, _, err := t.readSectionRows(sec)
		if err != nil {
			continue
		}
		err = t.indexDB.Update(func(tx *bbolt.Tx) error {
			fwd := tx.Bucket([]byte(fieldIndexBucket))
			rev := tx.Bucket([]byte(reverseFieldIndexBucket))
			if fwd == nil || rev == nil {
				return fmt.Errorf("missing field index buckets")
			}
			for _, r := range bySection[sec] {
				if int(r.loc.Offset) >= len(secRows) {
					continue
				}
				row := secRows[int(r.loc.Offset)]
				fieldVal := tpath.PathLookup(row, field)
				if fieldVal == nil {
					continue
				}
				valueBytes, ok := encodeIndexValue(fieldVal)
				if !ok {
					continue
				}
				if err := fwd.Put(makeFieldIndexKey(field, valueBytes, r.id), encodeRowLoc(r.loc)); err != nil {
					return err
				}
				if err := rev.Put(makeReverseFieldIndexKey(field, r.id), valueBytes); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *ArrowTable) EnsureFieldIndex(field string) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.indexedFields == nil {
		t.indexedFields = map[string]struct{}{}
	}
	t.indexedFields[field] = struct{}{}
	if err := t.indexDB.Update(func(tx *bbolt.Tx) error {
		if err := t.persistIndexedFieldsLocked(tx); err != nil {
			return err
		}
		return t.removeFieldIndexLocked(tx, field)
	}); err != nil {
		return err
	}
	if err := t.buildFieldIndexLocked(field); err != nil {
		return err
	}
	t.invalidateExecutionCaches()
	return nil
}

func (t *ArrowTable) RemoveFieldIndex(field string) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.indexedFields, field)
	err := t.indexDB.Update(func(tx *bbolt.Tx) error {
		if err := t.persistIndexedFieldsLocked(tx); err != nil {
			return err
		}
		return t.removeFieldIndexLocked(tx, field)
	})
	if err == nil {
		t.invalidateExecutionCaches()
	}
	return err
}

func (t *ArrowTable) IndexedFields() []string {
	t.lock.RLock()
	defer t.lock.RUnlock()
	out := make([]string, 0, len(t.indexedFields))
	for f := range t.indexedFields {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

func encodeRowLoc(loc *benchtop.RowLoc) []byte {
	out := make([]byte, 14)
	binary.LittleEndian.PutUint16(out[0:2], loc.TableId)
	binary.LittleEndian.PutUint16(out[2:4], loc.Section)
	binary.LittleEndian.PutUint32(out[4:8], loc.Offset)
	binary.LittleEndian.PutUint32(out[8:12], loc.Size)
	binary.LittleEndian.PutUint16(out[12:14], loc.Index)
	return out
}

func decodeRowLoc(v []byte) (*benchtop.RowLoc, error) {
	if len(v) < 14 {
		return nil, fmt.Errorf("invalid row loc length: %d", len(v))
	}
	return &benchtop.RowLoc{
		TableId: binary.LittleEndian.Uint16(v[0:2]),
		Section: binary.LittleEndian.Uint16(v[2:4]),
		Offset:  binary.LittleEndian.Uint32(v[4:8]),
		Size:    binary.LittleEndian.Uint32(v[8:12]),
		Index:   binary.LittleEndian.Uint16(v[12:14]),
	}, nil
}

func (t *ArrowTable) listIndexRows() ([]indexedRow, error) {
	out := []indexedRow{}
	err := t.indexDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(idsBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			loc, err := decodeRowLoc(v)
			if err != nil {
				return err
			}
			out = append(out, indexedRow{id: string(k), loc: loc})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].loc.Section == out[j].loc.Section {
			if out[i].loc.Offset == out[j].loc.Offset {
				return out[i].id < out[j].id
			}
			return out[i].loc.Offset < out[j].loc.Offset
		}
		return out[i].loc.Section < out[j].loc.Section
	})
	return out, nil
}

func (t *ArrowTable) activeRowLocs() (map[string]*benchtop.RowLoc, error) {
	out := map[string]*benchtop.RowLoc{}
	err := t.indexDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(idsBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			loc, err := decodeRowLoc(v)
			if err != nil {
				return err
			}
			out[string(k)] = loc
			return nil
		})
	})
	return out, err
}

func (t *ArrowTable) nextSection() (uint16, error) {
	var next uint16
	err := t.indexDB.View(func(tx *bbolt.Tx) error {
		mb := tx.Bucket([]byte(metaBucket))
		if mb == nil {
			next = 0
			return nil
		}
		v := mb.Get([]byte(metaNextSecKey))
		if len(v) >= 2 {
			next = binary.LittleEndian.Uint16(v)
		}
		return nil
	})
	return next, err
}

func (t *ArrowTable) ScanIndex(fn func(id string, loc *benchtop.RowLoc)) error {
	return t.indexDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(idsBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			loc, err := decodeRowLoc(v)
			if err != nil {
				return err
			}
			fn(string(k), loc)
			return nil
		})
	})
}
