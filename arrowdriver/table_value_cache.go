package arrowdriver

import (
	"bytes"
	"sort"

	"go.etcd.io/bbolt"
)

func (t *ArrowTable) invalidateValueIndexCache() {
	t.valueIndexCacheLock.Lock()
	defer t.valueIndexCacheLock.Unlock()
	t.valueIndexCache = map[string]map[string][]indexedRow{}
}

func cloneIndexedRows(in []indexedRow) []indexedRow {
	out := make([]indexedRow, len(in))
	copy(out, in)
	return out
}

func (t *ArrowTable) getCachedValuePostings(field string, valueBytes []byte) ([]indexedRow, bool) {
	key := string(valueBytes)
	t.valueIndexCacheLock.RLock()
	defer t.valueIndexCacheLock.RUnlock()
	fm, ok := t.valueIndexCache[field]
	if !ok {
		return nil, false
	}
	rows, ok := fm[key]
	if !ok {
		return nil, false
	}
	return cloneIndexedRows(rows), true
}

func (t *ArrowTable) setCachedValuePostings(field string, valueBytes []byte, rows []indexedRow) {
	key := string(valueBytes)
	cloned := cloneIndexedRows(rows)
	t.valueIndexCacheLock.Lock()
	defer t.valueIndexCacheLock.Unlock()
	fm, ok := t.valueIndexCache[field]
	if !ok {
		fm = map[string][]indexedRow{}
		t.valueIndexCache[field] = fm
	}
	fm[key] = cloned
}

func (t *ArrowTable) loadValuePostings(field string, valueBytes []byte) ([]indexedRow, int) {
	out := make([]indexedRow, 0, 256)
	missingLoc := []string{}
	_ = t.indexDB.View(func(tx *bbolt.Tx) error {
		fwd := tx.Bucket([]byte(fieldIndexBucket))
		if fwd == nil {
			return nil
		}
		c := fwd.Cursor()
		prefix := append(makeFieldIndexPrefix(field, valueBytes), 0x1f)
		for k, rawLoc := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, rawLoc = c.Next() {
			id, ok := indexedIDFromKey(k)
			if !ok {
				continue
			}
			loc, err := decodeRowLoc(rawLoc)
			if err != nil || loc == nil {
				missingLoc = append(missingLoc, id)
				continue
			}
			out = append(out, indexedRow{id: id, loc: loc})
		}
		return nil
	})

	for _, id := range missingLoc {
		loc, err := t.GetRowLoc(id)
		if err != nil {
			continue
		}
		out = append(out, indexedRow{id: id, loc: loc})
	}

	// Keep section/offset order for downstream read locality.
	sort.Slice(out, func(i, j int) bool {
		if out[i].loc.Section == out[j].loc.Section {
			if out[i].loc.Offset == out[j].loc.Offset {
				return out[i].id < out[j].id
			}
			return out[i].loc.Offset < out[j].loc.Offset
		}
		return out[i].loc.Section < out[j].loc.Section
	})
	return out, len(missingLoc)
}

func (t *ArrowTable) getOrLoadValuePostings(field string, valueBytes []byte) ([]indexedRow, int) {
	if rows, ok := t.getCachedValuePostings(field, valueBytes); ok {
		return rows, 0
	}
	rows, missing := t.loadValuePostings(field, valueBytes)
	t.setCachedValuePostings(field, valueBytes, rows)
	return rows, missing
}

