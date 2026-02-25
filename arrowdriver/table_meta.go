package arrowdriver

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/bmeg/benchtop"
	"github.com/bytedance/sonic"
	"go.etcd.io/bbolt"
)

func isIndexableValue(v any) bool {
	if v == nil {
		return false
	}
	switch v.(type) {
	case string, bool:
		return true
	}
	return isNumeric(v)
}

func collectIndexableFlatValues(data map[string]any) map[string]any {
	flat := flattenRowData(data)
	out := make(map[string]any, len(flat))
	for k, v := range flat {
		if strings.Contains(k, ".") || strings.Contains(k, "[") {
			continue
		}
		if !isIndexableValue(v) {
			continue
		}
		out[k] = v
	}
	return out
}

type sectionRowData struct {
	id      string
	payload string
	cols    map[string]any
}

func isNumeric(v any) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64:
		return true
	case uint, uint8, uint16, uint32, uint64:
		return true
	case float32, float64:
		return true
	default:
		return false
	}
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

func flattenRowData(data map[string]any) map[string]any {
	out := make(map[string]any, len(data))
	for k, v := range data {
		out[k] = v
	}
	return out
}

func buildSectionRows(rows []benchtop.Row) ([]sectionRowData, error) {
	out := make([]sectionRowData, len(rows))
	for i, row := range rows {
		payload, err := sonic.ConfigFastest.Marshal(row.Data)
		if err != nil {
			return nil, err
		}
		out[i] = sectionRowData{
			id:      string(row.Id),
			payload: string(payload),
			cols:    flattenRowData(row.Data),
		}
	}
	return out, nil
}

func detectEncoding(rows []sectionRowData, key string) columnEncoding {
	seen := false
	onlyString := true
	onlyBool := true
	onlyNumeric := true
	for _, row := range rows {
		v, ok := row.cols[key]
		if !ok || v == nil {
			continue
		}
		seen = true
		if _, ok := v.(string); !ok {
			onlyString = false
		}
		if _, ok := v.(bool); !ok {
			onlyBool = false
		}
		if !isNumeric(v) {
			onlyNumeric = false
		}
	}
	if !seen {
		return encJSON
	}
	if onlyString {
		return encString
	}
	if onlyBool {
		return encBool
	}
	if onlyNumeric {
		return encFloat64
	}
	return encJSON
}

func inferSectionColumns(rows []sectionRowData) ([]string, map[string]columnEncoding) {
	keySet := map[string]struct{}{}
	for _, row := range rows {
		for k := range row.cols {
			if k == idColumn || k == dataColumn {
				continue
			}
			keySet[k] = struct{}{}
		}
	}
	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	enc := make(map[string]columnEncoding, len(keys))
	for _, k := range keys {
		enc[k] = detectEncoding(rows, k)
	}
	return keys, enc
}

func (t *ArrowTable) SetWriteHints(keys []string, enc map[string]columnEncoding, strict bool) {
	keySet := map[string]struct{}{}
	outKeys := make([]string, 0, len(keys))
	for _, k := range keys {
		if k == "" || k == idColumn || k == dataColumn {
			continue
		}
		if _, ok := keySet[k]; ok {
			continue
		}
		keySet[k] = struct{}{}
		outKeys = append(outKeys, k)
	}
	sort.Strings(outKeys)
	outEnc := map[string]columnEncoding{}
	for k, v := range enc {
		if _, ok := keySet[k]; ok {
			outEnc[k] = v
		}
	}

	t.lock.Lock()
	defer t.lock.Unlock()
	t.writeHintKeys = outKeys
	t.writeHintEnc = outEnc
	t.writeHintOnly = strict
}

func (t *ArrowTable) inferSectionColumns(rows []sectionRowData) ([]string, map[string]columnEncoding) {
	hintKeys := append([]string(nil), t.writeHintKeys...)
	hintEnc := map[string]columnEncoding{}
	for k, v := range t.writeHintEnc {
		hintEnc[k] = v
	}
	hintOnly := t.writeHintOnly

	if len(hintKeys) == 0 {
		return inferSectionColumns(rows)
	}

	keys := append([]string(nil), hintKeys...)
	if !hintOnly {
		keySet := map[string]struct{}{}
		for _, k := range keys {
			keySet[k] = struct{}{}
		}
		for _, row := range rows {
			for k := range row.cols {
				if k == idColumn || k == dataColumn {
					continue
				}
				if _, ok := keySet[k]; ok {
					continue
				}
				keySet[k] = struct{}{}
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
	}

	enc := make(map[string]columnEncoding, len(keys))
	for _, k := range keys {
		if v, ok := hintEnc[k]; ok {
			enc[k] = v
			continue
		}
		enc[k] = detectEncoding(rows, k)
	}

	// Structural fields are used heavily by graph traversals and should
	// remain materialized even with schema hints.
	need := []string{"_from", "_to", "_label"}
	seen := map[string]struct{}{}
	for _, k := range keys {
		seen[k] = struct{}{}
	}
	for _, k := range need {
		if _, ok := seen[k]; ok {
			continue
		}
		keys = append(keys, k)
		enc[k] = detectEncoding(rows, k)
	}
	sort.Strings(keys)
	return keys, enc
}

func newArrowTable(baseDir, name string, tableID uint16, columns []benchtop.ColumnDef) (*ArrowTable, error) {
	indexPath := filepath.Join(baseDir, name+indexFileExt)

	db, err := bbolt.Open(indexPath, 0600, nil)
	if err != nil {
		return nil, err
	}

	t := &ArrowTable{
		name:               name,
		baseDir:            baseDir,
		tableID:            tableID,
		columns:            columns,
		schema:             arrow.NewSchema([]arrow.Field{{Name: idColumn, Type: arrow.BinaryTypes.String}}, nil),
		indexedFields:      map[string]struct{}{},
		indexPath:          indexPath,
		indexDB:            db,
		lock:               sync.RWMutex{},
		columnCache:        map[string][]any{},
		columnCacheCap:     256,
		sectionRowCache:    map[uint16][]map[string]any{},
		sectionRowCacheCap: 24,
		valueIndexCache:    map[string]map[string][]indexedRow{},
		rowOrdinalByID:     map[string]uint32{},
	}
	if len(columns) > 0 {
		keys := make([]string, 0, len(columns))
		for _, c := range columns {
			if c.Key == "" || c.Key == idColumn || c.Key == dataColumn {
				continue
			}
			keys = append(keys, c.Key)
		}
		t.writeHintKeys = keys
		t.writeHintEnc = map[string]columnEncoding{}
		t.writeHintOnly = false
	}

	if err := t.indexDB.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(idsBucket)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(fieldIndexBucket)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(reverseFieldIndexBucket)); err != nil {
			return err
		}
		mb, err := tx.CreateBucketIfNotExists([]byte(metaBucket))
		if err != nil {
			return err
		}
		if mb.Get([]byte(metaTableIDKey)) == nil {
			v := make([]byte, 2)
			binary.LittleEndian.PutUint16(v, tableID)
			if err := mb.Put([]byte(metaTableIDKey), v); err != nil {
				return err
			}
		}
		if mb.Get([]byte(metaNextSecKey)) == nil {
			v := make([]byte, 2)
			binary.LittleEndian.PutUint16(v, 0)
			if err := mb.Put([]byte(metaNextSecKey), v); err != nil {
				return err
			}
		}
		colBytes, err := sonic.ConfigFastest.Marshal(columns)
		if err != nil {
			return err
		}
		if err := mb.Put([]byte(metaColumnsKey), colBytes); err != nil {
			return err
		}
		if mb.Get([]byte(metaIndexKey)) == nil {
			idxBytes, err := sonic.ConfigFastest.Marshal([]string{})
			if err != nil {
				return err
			}
			if err := mb.Put([]byte(metaIndexKey), idxBytes); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}

	return t, nil
}

func loadArrowTable(baseDir, name string) (*ArrowTable, error) {
	indexPath := filepath.Join(baseDir, name+indexFileExt)
	db, err := bbolt.Open(indexPath, 0600, nil)
	if err != nil {
		return nil, err
	}

	t := &ArrowTable{
		name:               name,
		baseDir:            baseDir,
		schema:             arrow.NewSchema([]arrow.Field{{Name: idColumn, Type: arrow.BinaryTypes.String}}, nil),
		indexedFields:      map[string]struct{}{},
		indexPath:          indexPath,
		indexDB:            db,
		lock:               sync.RWMutex{},
		columnCache:        map[string][]any{},
		columnCacheCap:     256,
		sectionRowCache:    map[uint16][]map[string]any{},
		sectionRowCacheCap: 24,
		valueIndexCache:    map[string]map[string][]indexedRow{},
		rowOrdinalByID:     map[string]uint32{},
	}

	if err := t.indexDB.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(idsBucket)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(fieldIndexBucket)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(reverseFieldIndexBucket)); err != nil {
			return err
		}
		mb, err := tx.CreateBucketIfNotExists([]byte(metaBucket))
		if err != nil {
			return err
		}

		tid := mb.Get([]byte(metaTableIDKey))
		if tid == nil {
			v := make([]byte, 2)
			binary.LittleEndian.PutUint16(v, 0)
			if err := mb.Put([]byte(metaTableIDKey), v); err != nil {
				return err
			}
			t.tableID = 0
		} else {
			t.tableID = binary.LittleEndian.Uint16(tid)
		}

		next := mb.Get([]byte(metaNextSecKey))
		if next == nil {
			v := make([]byte, 2)
			binary.LittleEndian.PutUint16(v, t.inferNextSectionUnsafe())
			if err := mb.Put([]byte(metaNextSecKey), v); err != nil {
				return err
			}
		}

		colBytes := mb.Get([]byte(metaColumnsKey))
		if len(colBytes) > 0 {
			var cols []benchtop.ColumnDef
			if err := sonic.ConfigFastest.Unmarshal(colBytes, &cols); err == nil {
				t.columns = cols
			}
		}
		idxBytes := mb.Get([]byte(metaIndexKey))
		if len(idxBytes) > 0 {
			var idxFields []string
			if err := sonic.ConfigFastest.Unmarshal(idxBytes, &idxFields); err == nil {
				for _, f := range idxFields {
					t.indexedFields[f] = struct{}{}
				}
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}

	return t, nil
}

func (t *ArrowTable) Close() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.indexDB != nil {
		return t.indexDB.Close()
	}
	return nil
}

func (t *ArrowTable) sectionPath(section uint16) string {
	return filepath.Join(t.baseDir, fmt.Sprintf("%s_%06d%s", t.name, section, arrowFileExt))
}

func (t *ArrowTable) inferNextSectionUnsafe() uint16 {
	prefix := t.name + "_"
	pattern := filepath.Join(t.baseDir, t.name+"_*"+arrowFileExt)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return 0
	}
	maxSection := -1
	for _, p := range files {
		base := filepath.Base(p)
		if !strings.HasPrefix(base, prefix) || !strings.HasSuffix(base, arrowFileExt) {
			continue
		}
		mid := strings.TrimSuffix(strings.TrimPrefix(base, prefix), arrowFileExt)
		var sec int
		if _, err := fmt.Sscanf(mid, "%d", &sec); err != nil {
			continue
		}
		if sec > maxSection {
			maxSection = sec
		}
	}
	if maxSection < 0 {
		return 0
	}
	if maxSection >= int(^uint16(0)) {
		return ^uint16(0)
	}
	return uint16(maxSection + 1)
}

func (t *ArrowTable) reserveSection() (uint16, error) {
	var section uint16
	err := t.indexDB.Update(func(tx *bbolt.Tx) error {
		mb := tx.Bucket([]byte(metaBucket))
		if mb == nil {
			return fmt.Errorf("missing meta bucket")
		}
		next := mb.Get([]byte(metaNextSecKey))
		if len(next) < 2 {
			section = t.inferNextSectionUnsafe()
		} else {
			section = binary.LittleEndian.Uint16(next)
		}
		v := make([]byte, 2)
		binary.LittleEndian.PutUint16(v, section+1)
		return mb.Put([]byte(metaNextSecKey), v)
	})
	return section, err
}
