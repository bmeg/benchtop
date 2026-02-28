package arrowdriver

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/bmeg/benchtop"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"go.etcd.io/bbolt"
)

func decodeJSONTextCell(v string) any {
	if len(v) == 0 {
		return v
	}
	// Complex values are stored as JSON text in Arrow string columns.
	// Decode array/object payloads so filter semantics (e.g. CONTAINS) match JSON driver behavior.
	if v[0] != '[' && v[0] != '{' {
		return v
	}
	var out any
	if err := sonic.ConfigFastest.Unmarshal([]byte(v), &out); err != nil {
		return v
	}
	return out
}

func (t *ArrowTable) sectionColumnCacheKey(section uint16, field string) string {
	return fmt.Sprintf("%d|%s", section, field)
}

func (t *ArrowTable) getCachedSectionColumn(section uint16, field string) ([]any, bool) {
	key := t.sectionColumnCacheKey(section, field)
	t.columnCacheLock.RLock()
	defer t.columnCacheLock.RUnlock()
	v, ok := t.columnCache[key]
	return v, ok
}

func (t *ArrowTable) setCachedSectionColumn(section uint16, field string, vals []any) {
	if t.columnCacheCap <= 0 {
		return
	}
	key := t.sectionColumnCacheKey(section, field)
	t.columnCacheLock.Lock()
	defer t.columnCacheLock.Unlock()
	if t.columnCache == nil {
		t.columnCache = map[string][]any{}
	}
	if _, exists := t.columnCache[key]; !exists {
		if len(t.columnCacheOrder) >= t.columnCacheCap {
			evict := t.columnCacheOrder[0]
			t.columnCacheOrder = t.columnCacheOrder[1:]
			delete(t.columnCache, evict)
		}
		t.columnCacheOrder = append(t.columnCacheOrder, key)
	}
	t.columnCache[key] = vals
}

func cloneRowMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (t *ArrowTable) getCachedSectionRows(section uint16) ([]map[string]any, bool) {
	t.sectionRowCacheLock.RLock()
	defer t.sectionRowCacheLock.RUnlock()
	rows, ok := t.sectionRowCache[section]
	if !ok || len(rows) == 0 {
		return nil, false
	}
	return rows, true
}

func (t *ArrowTable) setCachedSectionRows(section uint16, rows []map[string]any) {
	if t.sectionRowCacheCap <= 0 || len(rows) == 0 {
		return
	}
	t.sectionRowCacheLock.Lock()
	defer t.sectionRowCacheLock.Unlock()
	if t.sectionRowCache == nil {
		t.sectionRowCache = map[uint16][]map[string]any{}
	}
	if _, exists := t.sectionRowCache[section]; !exists {
		if len(t.sectionRowCacheOrder) >= t.sectionRowCacheCap {
			evict := t.sectionRowCacheOrder[0]
			t.sectionRowCacheOrder = t.sectionRowCacheOrder[1:]
			delete(t.sectionRowCache, evict)
		}
		t.sectionRowCacheOrder = append(t.sectionRowCacheOrder, section)
	}
	t.sectionRowCache[section] = rows
}

func (t *ArrowTable) invalidateSectionCaches(section uint16) {
	t.columnCacheLock.Lock()
	for k := range t.columnCache {
		if strings.HasPrefix(k, fmt.Sprintf("%d|", section)) {
			delete(t.columnCache, k)
		}
	}
	t.columnCacheOrder = t.columnCacheOrder[:0]
	for k := range t.columnCache {
		t.columnCacheOrder = append(t.columnCacheOrder, k)
	}
	t.columnCacheLock.Unlock()

	t.sectionRowCacheLock.Lock()
	delete(t.sectionRowCache, section)
	t.sectionRowCacheOrder = t.sectionRowCacheOrder[:0]
	for k := range t.sectionRowCache {
		t.sectionRowCacheOrder = append(t.sectionRowCacheOrder, k)
	}
	t.sectionRowCacheLock.Unlock()
}

func (t *ArrowTable) clearAllSectionCaches() {
	t.columnCacheLock.Lock()
	t.columnCache = map[string][]any{}
	t.columnCacheOrder = nil
	t.columnCacheLock.Unlock()

	t.sectionRowCacheLock.Lock()
	t.sectionRowCache = map[uint16][]map[string]any{}
	t.sectionRowCacheOrder = nil
	t.sectionRowCacheLock.Unlock()
}

func isTopLevelMaterializedField(field string) bool {
	if field == "" || field == idColumn || field == dataColumn {
		return false
	}
	return !strings.Contains(field, ".") && !strings.Contains(field, "[")
}

func decodeRecordValue(col arrow.Array, i int) any {
	switch arr := col.(type) {
	case *array.String:
		if arr.IsNull(i) {
			return nil
		}
		return decodeJSONTextCell(arr.Value(i))
	case *array.Float64:
		if arr.IsNull(i) {
			return nil
		}
		return arr.Value(i)
	case *array.Boolean:
		if arr.IsNull(i) {
			return nil
		}
		return arr.Value(i)
	default:
		return nil
	}
}

func (t *ArrowTable) writeSectionMaterialized(section uint16, sectionRows []sectionRowData) error {
	t.invalidateSectionCaches(section)
	f, err := os.Create(t.sectionPath(section))
	if err != nil {
		return err
	}
	defer f.Close()

	keys, enc := t.inferSectionColumns(sectionRows)
	fields := make([]arrow.Field, 0, 2+len(keys))
	fields = append(fields,
		arrow.Field{Name: idColumn, Type: arrow.BinaryTypes.String},
		arrow.Field{Name: dataColumn, Type: arrow.BinaryTypes.String},
	)
	for _, k := range keys {
		switch enc[k] {
		case encFloat64:
			fields = append(fields, arrow.Field{Name: k, Type: arrow.PrimitiveTypes.Float64, Nullable: true})
		case encBool:
			fields = append(fields, arrow.Field{Name: k, Type: arrow.FixedWidthTypes.Boolean, Nullable: true})
		default:
			// encString and encJSON are persisted as UTF-8 string columns.
			fields = append(fields, arrow.Field{Name: k, Type: arrow.BinaryTypes.String, Nullable: true})
		}
	}
	schema := arrow.NewSchema(fields, nil)

	mem := memory.NewGoAllocator()
	writer := ipc.NewWriter(f, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	defer writer.Close()

	for start := 0; start < len(sectionRows); start += sectionWriteBatchRows {
		end := start + sectionWriteBatchRows
		if end > len(sectionRows) {
			end = len(sectionRows)
		}

		idb := array.NewStringBuilder(mem)
		db := array.NewStringBuilder(mem)
		strBuilders := map[string]*array.StringBuilder{}
		floatBuilders := map[string]*array.Float64Builder{}
		boolBuilders := map[string]*array.BooleanBuilder{}
		for _, k := range keys {
			switch enc[k] {
			case encFloat64:
				floatBuilders[k] = array.NewFloat64Builder(mem)
			case encBool:
				boolBuilders[k] = array.NewBooleanBuilder(mem)
			default:
				strBuilders[k] = array.NewStringBuilder(mem)
			}
		}

		for _, row := range sectionRows[start:end] {
			idb.Append(row.id)
			db.Append(row.payload)

			for _, k := range keys {
				v, ok := row.cols[k]
				if !ok || v == nil {
					if b := strBuilders[k]; b != nil {
						b.AppendNull()
					}
					if b := floatBuilders[k]; b != nil {
						b.AppendNull()
					}
					if b := boolBuilders[k]; b != nil {
						b.AppendNull()
					}
					continue
				}

				if b := floatBuilders[k]; b != nil {
					if fv, ok := toFloat64(v); ok {
						b.Append(fv)
					} else {
						b.AppendNull()
					}
					continue
				}
				if b := boolBuilders[k]; b != nil {
					if bv, ok := v.(bool); ok {
						b.Append(bv)
					} else {
						b.AppendNull()
					}
					continue
				}
				if b := strBuilders[k]; b != nil {
					if enc[k] == encString {
						if sv, ok := v.(string); ok {
							b.Append(sv)
						} else {
							b.AppendNull()
						}
						continue
					}
					// Complex value encoded as JSON text.
					jv, err := sonic.ConfigFastest.Marshal(v)
					if err != nil {
						b.AppendNull()
						continue
					}
					b.Append(string(jv))
				}
			}
		}

		arrays := make([]arrow.Array, 0, 2+len(keys))
		ids := idb.NewArray()
		data := db.NewArray()
		arrays = append(arrays, ids, data)
		for _, k := range keys {
			if b := floatBuilders[k]; b != nil {
				arrays = append(arrays, b.NewArray())
				continue
			}
			if b := boolBuilders[k]; b != nil {
				arrays = append(arrays, b.NewArray())
				continue
			}
			arrays = append(arrays, strBuilders[k].NewArray())
		}

		rec := array.NewRecord(schema, arrays, int64(end-start))
		err := writer.Write(rec)
		rec.Release()
		for _, a := range arrays {
			a.Release()
		}
		idb.Release()
		db.Release()
		for _, b := range strBuilders {
			b.Release()
		}
		for _, b := range floatBuilders {
			b.Release()
		}
		for _, b := range boolBuilders {
			b.Release()
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *ArrowTable) writeSection(section uint16, rows []benchtop.Row) error {
	sectionRows, err := buildSectionRows(rows)
	if err != nil {
		return err
	}
	return t.writeSectionMaterialized(section, sectionRows)
}

func (t *ArrowTable) BulkLoad(rows []benchtop.Row) error {
	if len(rows) == 0 {
		return nil
	}
	start := time.Now()
	t.lock.Lock()
	defer t.lock.Unlock()

	section, err := t.reserveSection()
	if err != nil {
		return err
	}

	sectionRows, err := buildSectionRows(rows)
	if err != nil {
		return err
	}

	writeStart := time.Now()
	if err := t.writeSectionMaterialized(section, sectionRows); err != nil {
		return err
	}
	writeElapsed := time.Since(writeStart).Round(time.Millisecond)

	indexableRows := make([]map[string]any, len(rows))
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
	log.Debugf("arrowtable.BulkLoad section_written table=%s tableID=%d section=%d rows=%d indexedFields=%d writeElapsed=%s", t.name, t.tableID, section, len(rows), len(seenFields), writeElapsed)

	indexStart := time.Now()
	indexWrites := 0
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
		for i, row := range rows {
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
					indexWrites++
				}
			}
		}
		log.Debugf("arrowtable.BulkLoad section_indexed table=%s tableID=%d section=%d rows=%d indexWrites=%d indexElapsed=%s totalElapsed=%s", t.name, t.tableID, section, len(rows), indexWrites, time.Since(indexStart).Round(time.Millisecond), time.Since(start).Round(time.Millisecond))
		return nil
	})
	if err == nil {
		t.invalidateExecutionCaches()
	}
	return err
}

func (t *ArrowTable) streamSectionRows(section uint16, fn func(id string, data map[string]any, offset uint32) bool) error {
	f, err := os.Open(t.sectionPath(section))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	reader, err := ipc.NewReader(f)
	if err != nil {
		return err
	}
	defer reader.Release()

	var offset uint32 = 0
	for reader.Next() {
		rec := reader.Record()
		schema := rec.Schema()
		idIdx := schema.FieldIndices(idColumn)
		if len(idIdx) == 0 {
			return fmt.Errorf("missing _id column")
		}
		idArr, ok := rec.Column(idIdx[0]).(*array.String)
		if !ok {
			return fmt.Errorf("id column is not string")
		}
		topFields := make([]string, 0, len(schema.Fields()))
		topCols := make([]arrow.Array, 0, len(schema.Fields()))
		for idx, f := range schema.Fields() {
			if !isTopLevelMaterializedField(f.Name) {
				continue
			}
			topFields = append(topFields, f.Name)
			topCols = append(topCols, rec.Column(idx))
		}
		var dataArr *array.String
		if dataIdx := schema.FieldIndices(dataColumn); len(dataIdx) > 0 {
			if arr, ok := rec.Column(dataIdx[0]).(*array.String); ok {
				dataArr = arr
			}
		}
		for i := 0; i < int(rec.NumRows()); i++ {
			id := idArr.Value(i)
			var data map[string]any
			if dataArr != nil && !dataArr.IsNull(i) {
				_ = sonic.UnmarshalString(dataArr.Value(i), &data)
				if data == nil {
					data = map[string]any{}
				}
				data[idColumn] = id
			} else {
				data = map[string]any{idColumn: id}
				for cIdx, field := range topFields {
					if v := decodeRecordValue(topCols[cIdx], i); v != nil {
						data[field] = v
					}
				}
			}
			if !fn(id, data, offset) {
				return nil
			}
			offset++
		}
	}
	return nil
}

// ScanIndex iterates the bbolt index and emits (id, RowLoc) pairs without
// reading any row data. Used for efficient cache preloading.
func (t *ArrowTable) readSectionRows(section uint16) ([]map[string]any, []string, error) {
	if cached, ok := t.getCachedSectionRows(section); ok {
		rows := make([]map[string]any, len(cached))
		ids := make([]string, len(cached))
		for i, r := range cached {
			rows[i] = cloneRowMap(r)
			if id, ok := r[idColumn].(string); ok {
				ids[i] = id
			}
		}
		return rows, ids, nil
	}

	f, err := os.Open(t.sectionPath(section))
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	reader, err := ipc.NewReader(f)
	if err != nil {
		return nil, nil, err
	}
	defer reader.Release()

	rows := []map[string]any{}
	ids := []string{}
	for reader.Next() {
		rec := reader.Record()
		schema := rec.Schema()
		idIdx := schema.FieldIndices(idColumn)
		if len(idIdx) == 0 {
			return nil, nil, fmt.Errorf("missing _id column")
		}
		idArr, ok := rec.Column(idIdx[0]).(*array.String)
		if !ok {
			return nil, nil, fmt.Errorf("id column is not string")
		}
		topFields := make([]string, 0, len(schema.Fields()))
		topCols := make([]arrow.Array, 0, len(schema.Fields()))
		for idx, f := range schema.Fields() {
			if !isTopLevelMaterializedField(f.Name) {
				continue
			}
			topFields = append(topFields, f.Name)
			topCols = append(topCols, rec.Column(idx))
		}
		var dataArr *array.String
		if dataIdx := schema.FieldIndices(dataColumn); len(dataIdx) > 0 {
			if arr, ok := rec.Column(dataIdx[0]).(*array.String); ok {
				dataArr = arr
			}
		}
		for i := 0; i < int(rec.NumRows()); i++ {
			id := idArr.Value(i)
			var data map[string]any
			if dataArr != nil && !dataArr.IsNull(i) {
				if err := sonic.UnmarshalString(dataArr.Value(i), &data); err != nil {
					return nil, nil, err
				}
				if data == nil {
					data = map[string]any{}
				}
				data[idColumn] = id
			} else {
				data = map[string]any{idColumn: id}
				for cIdx, field := range topFields {
					if v := decodeRecordValue(topCols[cIdx], i); v != nil {
						data[field] = v
					}
				}
			}
			ids = append(ids, id)
			rows = append(rows, data)
		}
	}
	t.setCachedSectionRows(section, rows)

	// Return clones to ensure the caller cannot modify the maps stored in the cache.
	clonedRows := make([]map[string]any, len(rows))
	for i, r := range rows {
		clonedRows[i] = cloneRowMap(r)
	}
	return clonedRows, ids, nil
}

func (t *ArrowTable) readSectionTopLevelColumn(section uint16, field string) ([]any, bool, error) {
	if cached, ok := t.getCachedSectionColumn(section, field); ok {
		return cached, true, nil
	}

	f, err := os.Open(t.sectionPath(section))
	if err != nil {
		return nil, false, err
	}
	defer f.Close()

	reader, err := ipc.NewReader(f)
	if err != nil {
		return nil, false, err
	}
	defer reader.Release()

	out := []any{}
	found := false
	for reader.Next() {
		rec := reader.Record()
		idx := rec.Schema().FieldIndices(field)
		if len(idx) == 0 {
			return nil, false, nil
		}
		found = true
		col := rec.Column(idx[0])
		switch arr := col.(type) {
		case *array.String:
			for i := 0; i < int(rec.NumRows()); i++ {
				if arr.IsNull(i) {
					out = append(out, nil)
				} else {
					out = append(out, decodeJSONTextCell(arr.Value(i)))
				}
			}
		case *array.Float64:
			for i := 0; i < int(rec.NumRows()); i++ {
				if arr.IsNull(i) {
					out = append(out, nil)
				} else {
					out = append(out, arr.Value(i))
				}
			}
		case *array.Boolean:
			for i := 0; i < int(rec.NumRows()); i++ {
				if arr.IsNull(i) {
					out = append(out, nil)
				} else {
					out = append(out, arr.Value(i))
				}
			}
		default:
			return nil, false, nil
		}
	}
	if found {
		t.setCachedSectionColumn(section, field, out)
	}
	return out, found, nil
}

func (t *ArrowTable) readSectionProjectedRows(section uint16, fields []string) ([]map[string]any, error) {
	f, err := os.Open(t.sectionPath(section))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader, err := ipc.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	out := []map[string]any{}
	for reader.Next() {
		rec := reader.Record()
		schema := rec.Schema()
		idIdx := schema.FieldIndices(idColumn)
		if len(idIdx) == 0 {
			return nil, fmt.Errorf("missing _id column")
		}
		idArr, ok := rec.Column(idIdx[0]).(*array.String)
		if !ok {
			return nil, fmt.Errorf("_id column is not string")
		}

		fieldIdx := map[string]int{}
		for _, field := range fields {
			if field == idColumn {
				continue
			}
			idx := schema.FieldIndices(field)
			if len(idx) == 0 {
				fieldIdx[field] = -1
			} else {
				fieldIdx[field] = idx[0]
			}
		}

		for i := 0; i < int(rec.NumRows()); i++ {
			row := map[string]any{"_id": idArr.Value(i)}
			for _, field := range fields {
				if field == idColumn {
					continue
				}
				idx := fieldIdx[field]
				if idx < 0 {
					continue
				}
				col := rec.Column(idx)
				switch arr := col.(type) {
				case *array.String:
					if arr.IsNull(i) {
						continue
					}
					row[field] = arr.Value(i)
				case *array.Float64:
					if arr.IsNull(i) {
						continue
					}
					row[field] = arr.Value(i)
				case *array.Boolean:
					if arr.IsNull(i) {
						continue
					}
					row[field] = arr.Value(i)
				}
			}
			out = append(out, row)
		}
	}
	return out, nil
}
func (t *ArrowTable) readSectionTopLevelColumns(section uint16, fields []string) ([]string, map[string][]any, error) {
	requiredFields := []string{}
	for _, field := range fields {
		if field == "" || field == "_label" {
			continue
		}
		requiredFields = append(requiredFields, field)
	}

	cachedIDsAny, idCached := t.getCachedSectionColumn(section, idColumn)
	cachedCols := map[string][]any{}
	missingFields := []string{}
	for _, field := range requiredFields {
		if field == idColumn {
			continue
		}
		if v, ok := t.getCachedSectionColumn(section, field); ok {
			cachedCols[field] = v
		} else {
			missingFields = append(missingFields, field)
		}
	}

	if idCached && len(missingFields) == 0 {
		outIDs := make([]string, 0, len(cachedIDsAny))
		for _, v := range cachedIDsAny {
			if s, ok := v.(string); ok {
				outIDs = append(outIDs, s)
			} else {
				outIDs = append(outIDs, "")
			}
		}
		return outIDs, cachedCols, nil
	}

	f, err := os.Open(t.sectionPath(section))
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	reader, err := ipc.NewReader(f)
	if err != nil {
		return nil, nil, err
	}
	defer reader.Release()

	outIDsAny := []any{}
	outCols := map[string][]any{}
	for k, v := range cachedCols {
		outCols[k] = v
	}
	missingSet := map[string]struct{}{}
	for _, field := range missingFields {
		missingSet[field] = struct{}{}
		outCols[field] = []any{}
	}

	for reader.Next() {
		rec := reader.Record()
		schema := rec.Schema()
		idIdx := schema.FieldIndices(idColumn)
		if len(idIdx) == 0 {
			return nil, nil, fmt.Errorf("missing _id column")
		}
		idArr, ok := rec.Column(idIdx[0]).(*array.String)
		if !ok {
			return nil, nil, fmt.Errorf("id column is not string")
		}
		n := int(rec.NumRows())
		if !idCached {
			for i := 0; i < n; i++ {
				outIDsAny = append(outIDsAny, idArr.Value(i))
			}
		}
		for field := range missingSet {
			idx := schema.FieldIndices(field)
			if len(idx) == 0 {
				for i := 0; i < n; i++ {
					outCols[field] = append(outCols[field], nil)
				}
				continue
			}
			col := rec.Column(idx[0])
			switch arr := col.(type) {
			case *array.String:
				for i := 0; i < n; i++ {
					if arr.IsNull(i) {
						outCols[field] = append(outCols[field], nil)
					} else {
						outCols[field] = append(outCols[field], decodeJSONTextCell(arr.Value(i)))
					}
				}
			case *array.Float64:
				for i := 0; i < n; i++ {
					if arr.IsNull(i) {
						outCols[field] = append(outCols[field], nil)
					} else {
						outCols[field] = append(outCols[field], arr.Value(i))
					}
				}
			case *array.Boolean:
				for i := 0; i < n; i++ {
					if arr.IsNull(i) {
						outCols[field] = append(outCols[field], nil)
					} else {
						outCols[field] = append(outCols[field], arr.Value(i))
					}
				}
			default:
				for i := 0; i < n; i++ {
					outCols[field] = append(outCols[field], nil)
				}
			}
		}
	}

	if !idCached {
		t.setCachedSectionColumn(section, idColumn, outIDsAny)
		cachedIDsAny = outIDsAny
	}
	for field := range missingSet {
		t.setCachedSectionColumn(section, field, outCols[field])
	}

	outIDs := make([]string, 0, len(cachedIDsAny))
	for _, v := range cachedIDsAny {
		if s, ok := v.(string); ok {
			outIDs = append(outIDs, s)
		} else {
			outIDs = append(outIDs, "")
		}
	}

	return outIDs, outCols, nil
}

func (t *ArrowTable) readSectionRowsByOffsets(section uint16, offsets map[uint32]struct{}) (map[uint32]map[string]any, error) {
	if len(offsets) == 0 {
		return map[uint32]map[string]any{}, nil
	}
	if cached, ok := t.getCachedSectionRows(section); ok {
		out := map[uint32]map[string]any{}
		for off := range offsets {
			if int(off) < len(cached) {
				out[off] = cloneRowMap(cached[off])
			}
		}
		return out, nil
	}
	if len(offsets) >= 256 {
		rows, _, err := t.readSectionRows(section)
		if err == nil {
			out := map[uint32]map[string]any{}
			for off := range offsets {
				if int(off) < len(rows) {
					out[off] = cloneRowMap(rows[off])
				}
			}
			return out, nil
		}
	}
	maxOffset := uint32(0)
	remaining := len(offsets)
	for off := range offsets {
		if off > maxOffset {
			maxOffset = off
		}
	}

	f, err := os.Open(t.sectionPath(section))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader, err := ipc.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	out := map[uint32]map[string]any{}
	var offset uint32
	for reader.Next() {
		rec := reader.Record()
		schema := rec.Schema()
		idIdx := schema.FieldIndices(idColumn)
		if len(idIdx) == 0 {
			return nil, fmt.Errorf("missing _id column")
		}
		idArr, ok := rec.Column(idIdx[0]).(*array.String)
		if !ok {
			return nil, fmt.Errorf("id column is not string")
		}
		topFields := make([]string, 0, len(schema.Fields()))
		topCols := make([]arrow.Array, 0, len(schema.Fields()))
		for idx, f := range schema.Fields() {
			if !isTopLevelMaterializedField(f.Name) {
				continue
			}
			topFields = append(topFields, f.Name)
			topCols = append(topCols, rec.Column(idx))
		}
		var dataArr *array.String
		if dataIdx := schema.FieldIndices(dataColumn); len(dataIdx) > 0 {
			if arr, ok := rec.Column(dataIdx[0]).(*array.String); ok {
				dataArr = arr
			}
		}
		for i := 0; i < int(rec.NumRows()); i++ {
			if offset > maxOffset || remaining == 0 {
				return out, nil
			}
			if _, ok := offsets[offset]; !ok {
				offset++
				continue
			}
			var row map[string]any
			if dataArr != nil && !dataArr.IsNull(i) {
				if err := sonic.UnmarshalString(dataArr.Value(i), &row); err != nil {
					offset++
					continue
				}
				if row == nil {
					row = map[string]any{}
				}
				row[idColumn] = idArr.Value(i)
			} else {
				row = map[string]any{idColumn: idArr.Value(i)}
				for cIdx, field := range topFields {
					if v := decodeRecordValue(topCols[cIdx], i); v != nil {
						row[field] = v
					}
				}
			}
			out[offset] = row
			remaining--
			offset++
		}
	}
	return out, nil
}

func (t *ArrowTable) readSectionProjectedRowsByOffsets(section uint16, fields []string, offsets map[uint32]struct{}) (map[uint32]map[string]any, error) {
	if len(offsets) == 0 {
		return map[uint32]map[string]any{}, nil
	}
	maxOffset := uint32(0)
	remaining := len(offsets)
	for off := range offsets {
		if off > maxOffset {
			maxOffset = off
		}
	}

	f, err := os.Open(t.sectionPath(section))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader, err := ipc.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	out := map[uint32]map[string]any{}
	var offset uint32
	for reader.Next() {
		rec := reader.Record()
		schema := rec.Schema()
		idIdx := schema.FieldIndices(idColumn)
		if len(idIdx) == 0 {
			return nil, fmt.Errorf("missing _id column")
		}
		idArr, ok := rec.Column(idIdx[0]).(*array.String)
		if !ok {
			return nil, fmt.Errorf("_id column is not string")
		}

		fieldIdx := map[string]int{}
		for _, field := range fields {
			if field == idColumn {
				continue
			}
			idx := schema.FieldIndices(field)
			if len(idx) == 0 {
				fieldIdx[field] = -1
			} else {
				fieldIdx[field] = idx[0]
			}
		}

		for i := 0; i < int(rec.NumRows()); i++ {
			if offset > maxOffset || remaining == 0 {
				return out, nil
			}
			if _, ok := offsets[offset]; !ok {
				offset++
				continue
			}
			row := map[string]any{"_id": idArr.Value(i)}
			for _, field := range fields {
				if field == idColumn {
					continue
				}
				idx := fieldIdx[field]
				if idx < 0 {
					continue
				}
				col := rec.Column(idx)
				switch arr := col.(type) {
				case *array.String:
					if !arr.IsNull(i) {
						row[field] = arr.Value(i)
					}
				case *array.Float64:
					if !arr.IsNull(i) {
						row[field] = arr.Value(i)
					}
				case *array.Boolean:
					if !arr.IsNull(i) {
						row[field] = arr.Value(i)
					}
				}
			}
			out[offset] = row
			remaining--
			offset++
		}
	}
	return out, nil
}
