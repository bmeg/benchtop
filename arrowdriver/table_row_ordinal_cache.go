package arrowdriver

func (t *ArrowTable) invalidateRowOrdinalCache() {
	t.rowOrdinalCacheLock.Lock()
	defer t.rowOrdinalCacheLock.Unlock()
	t.rowOrdinalRows = nil
	t.rowOrdinalByID = map[string]uint32{}
}

func (t *ArrowTable) getOrLoadRowOrdinalCache() ([]indexedRow, map[string]uint32, error) {
	t.rowOrdinalCacheLock.RLock()
	if len(t.rowOrdinalRows) > 0 && len(t.rowOrdinalByID) > 0 {
		rows := t.rowOrdinalRows
		byID := t.rowOrdinalByID
		t.rowOrdinalCacheLock.RUnlock()
		return rows, byID, nil
	}
	t.rowOrdinalCacheLock.RUnlock()

	rows, err := t.listIndexRows()
	if err != nil {
		return nil, nil, err
	}
	byID := make(map[string]uint32, len(rows))
	for i, r := range rows {
		byID[r.id] = uint32(i)
	}

	t.rowOrdinalCacheLock.Lock()
	t.rowOrdinalRows = rows
	t.rowOrdinalByID = byID
	t.rowOrdinalCacheLock.Unlock()
	return rows, byID, nil
}

func (t *ArrowTable) invalidateExecutionCaches() {
	t.invalidateValueIndexCache()
	t.invalidateRowOrdinalCache()
}

