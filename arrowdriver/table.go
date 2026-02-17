package arrowdriver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	golog "log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/bmeg/benchtop"
	bfilters "github.com/bmeg/benchtop/filters"
	"github.com/bmeg/benchtop/jsontable/tpath"
	"github.com/bmeg/benchtop/query"
	"github.com/bmeg/benchtop/util"
	"github.com/bytedance/sonic"
	"go.etcd.io/bbolt"
)

const (
	arrowFileExt = ".arrow"
	indexFileExt = ".idx"
	idColumn     = "_id"
	dataColumn   = "_data"

	idsBucket      = "ids"
	metaBucket     = "meta"
	metaTableIDKey = "table_id"
	metaNextSecKey = "next_section"
	metaColumnsKey = "columns"
	metaIndexKey   = "indexed_fields"

	fieldIndexBucket        = "field_index"
	reverseFieldIndexBucket = "reverse_field_index"
)

const sectionWriteBatchRows = 1024

type columnEncoding uint8

const (
	encString columnEncoding = iota
	encFloat64
	encBool
	encJSON
)

type ArrowTable struct {
	name    string
	baseDir string
	tableID uint16
	columns []benchtop.ColumnDef
	schema  *arrow.Schema

	indexedFields map[string]struct{}

	indexPath string
	indexDB   *bbolt.DB
	lock      sync.RWMutex
}

type indexedRow struct {
	id  string
	loc *benchtop.RowLoc
}

const defaultCompactRowsPerSection = 50000

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

func detectEncoding(rows []benchtop.Row, key string) columnEncoding {
	seen := false
	onlyString := true
	onlyBool := true
	onlyNumeric := true
	for _, row := range rows {
		v, ok := row.Data[key]
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

func inferSectionColumns(rows []benchtop.Row) ([]string, map[string]columnEncoding) {
	keySet := map[string]struct{}{}
	for _, row := range rows {
		for k := range row.Data {
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

func newArrowTable(baseDir, name string, tableID uint16, columns []benchtop.ColumnDef) (*ArrowTable, error) {
	indexPath := filepath.Join(baseDir, name+indexFileExt)

	db, err := bbolt.Open(indexPath, 0600, nil)
	if err != nil {
		return nil, err
	}

	t := &ArrowTable{
		name:          name,
		baseDir:       baseDir,
		tableID:       tableID,
		columns:       columns,
		schema:        arrow.NewSchema([]arrow.Field{{Name: idColumn, Type: arrow.BinaryTypes.String}, {Name: dataColumn, Type: arrow.BinaryTypes.String}}, nil),
		indexedFields: map[string]struct{}{},
		indexPath:     indexPath,
		indexDB:       db,
		lock:          sync.RWMutex{},
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
		name:          name,
		baseDir:       baseDir,
		schema:        arrow.NewSchema([]arrow.Field{{Name: idColumn, Type: arrow.BinaryTypes.String}, {Name: dataColumn, Type: arrow.BinaryTypes.String}}, nil),
		indexedFields: map[string]struct{}{},
		indexPath:     indexPath,
		indexDB:       db,
		lock:          sync.RWMutex{},
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

func makeFieldIndexKey(field string, valueBytes []byte, rowID string) []byte {
	return bytes.Join([][]byte{[]byte(field), valueBytes, []byte(rowID)}, []byte{0x1f})
}

func makeFieldIndexPrefix(field string, valueBytes []byte) []byte {
	return bytes.Join([][]byte{[]byte(field), valueBytes}, []byte{0x1f})
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
				valueBytes, err := sonic.ConfigFastest.Marshal(fieldVal)
				if err != nil {
					continue
				}
				if err := fwd.Put(makeFieldIndexKey(field, valueBytes, r.id), nil); err != nil {
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
	return t.buildFieldIndexLocked(field)
}

func (t *ArrowTable) RemoveFieldIndex(field string) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.indexedFields, field)
	return t.indexDB.Update(func(tx *bbolt.Tx) error {
		if err := t.persistIndexedFieldsLocked(tx); err != nil {
			return err
		}
		return t.removeFieldIndexLocked(tx, field)
	})
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

func (t *ArrowTable) writeSection(section uint16, rows []benchtop.Row) error {
	f, err := os.Create(t.sectionPath(section))
	if err != nil {
		return err
	}
	defer f.Close()

	keys, enc := inferSectionColumns(rows)
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

	for start := 0; start < len(rows); start += sectionWriteBatchRows {
		end := start + sectionWriteBatchRows
		if end > len(rows) {
			end = len(rows)
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

		for _, row := range rows[start:end] {
			idb.Append(string(row.Id))
			payload, err := sonic.ConfigFastest.Marshal(row.Data)
			if err != nil {
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
				return err
			}
			db.Append(string(payload))

			for _, k := range keys {
				v, ok := row.Data[k]
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

func (t *ArrowTable) BulkLoad(rows []benchtop.Row) error {
	if len(rows) == 0 {
		return nil
	}
	t.lock.Lock()
	defer t.lock.Unlock()

	section, err := t.reserveSection()
	if err != nil {
		return err
	}

	if err := t.writeSection(section, rows); err != nil {
		return err
	}

	return t.indexDB.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(idsBucket))
		fwd := tx.Bucket([]byte(fieldIndexBucket))
		rev := tx.Bucket([]byte(reverseFieldIndexBucket))
		if b == nil {
			return fmt.Errorf("missing ids bucket")
		}
		indexed := make([]string, 0, len(t.indexedFields))
		for f := range t.indexedFields {
			indexed = append(indexed, f)
		}
		for i, row := range rows {
			loc := &benchtop.RowLoc{TableId: t.tableID, Section: section, Offset: uint32(i), Size: 0, Index: 0}
			if err := b.Put(row.Id, encodeRowLoc(loc)); err != nil {
				return err
			}
			if len(indexed) > 0 && fwd != nil && rev != nil {
				rowID := string(row.Id)
				for _, field := range indexed {
					fieldVal := tpath.PathLookup(row.Data, field)
					if fieldVal == nil {
						continue
					}
					valueBytes, err := sonic.ConfigFastest.Marshal(fieldVal)
					if err != nil {
						continue
					}
					if err := fwd.Put(makeFieldIndexKey(field, valueBytes, rowID), nil); err != nil {
						return err
					}
					if err := rev.Put(makeReverseFieldIndexKey(field, rowID), valueBytes); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
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
		idArr, ok := rec.Column(0).(*array.String)
		if !ok {
			return fmt.Errorf("id column is not string")
		}
		dataArr, ok := rec.Column(1).(*array.String)
		if !ok {
			return fmt.Errorf("data column is not string")
		}
		for i := 0; i < int(rec.NumRows()); i++ {
			id := idArr.Value(i)
			dataJSON := dataArr.Value(i)
			data := map[string]any{}
			if err := sonic.ConfigFastest.Unmarshal([]byte(dataJSON), &data); err != nil {
				offset++
				continue
			}
			data[idColumn] = id
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

func (t *ArrowTable) readSectionRows(section uint16) ([]map[string]any, []string, error) {
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
		idArr, ok := rec.Column(0).(*array.String)
		if !ok {
			return nil, nil, fmt.Errorf("id column is not string")
		}
		dataArr, ok := rec.Column(1).(*array.String)
		if !ok {
			return nil, nil, fmt.Errorf("data column is not string")
		}
		for i := 0; i < int(rec.NumRows()); i++ {
			id := idArr.Value(i)
			dataJSON := dataArr.Value(i)
			data := map[string]any{}
			if err := sonic.ConfigFastest.Unmarshal([]byte(dataJSON), &data); err != nil {
				return nil, nil, err
			}
			data[idColumn] = id
			ids = append(ids, id)
			rows = append(rows, data)
		}
	}
	return rows, ids, nil
}

func (t *ArrowTable) readSectionTopLevelColumn(section uint16, field string) ([]any, bool, error) {
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
					out = append(out, arr.Value(i))
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

func (t *ArrowTable) RowIdsByHas(field string, value any, op query.Condition) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		if op == query.EQ || op == query.WITHIN {
			t.lock.RLock()
			_, indexed := t.indexedFields[field]
			t.lock.RUnlock()
			if indexed {
				vals := []any{value}
				if op == query.WITHIN {
					vals = util.SliceToAny(value)
				}
				seen := map[string]struct{}{}
				_ = t.indexDB.View(func(tx *bbolt.Tx) error {
					fwd := tx.Bucket([]byte(fieldIndexBucket))
					if fwd == nil {
						return nil
					}
					c := fwd.Cursor()
					for _, v := range vals {
						valueBytes, err := sonic.ConfigFastest.Marshal(v)
						if err != nil {
							continue
						}
						prefix := append(makeFieldIndexPrefix(field, valueBytes), 0x1f)
						for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
							id, ok := indexedIDFromKey(k)
							if !ok {
								continue
							}
							if _, ok := seen[id]; ok {
								continue
							}
							seen[id] = struct{}{}
							out <- id
						}
					}
					return nil
				})
				return
			}
		}
		rows, err := t.listIndexRows()
		if err != nil {
			return
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

		topLevelField := !strings.Contains(field, ".")

		for _, secInt := range sections {
			sec := uint16(secInt)
			if topLevelField {
				colVals, found, err := t.readSectionTopLevelColumn(sec, field)
				if err == nil && found {
					for _, r := range bySection[sec] {
						if int(r.loc.Offset) >= len(colVals) {
							continue
						}
						if bfilters.ApplyFilterCondition(colVals[int(r.loc.Offset)], &query.FieldFilter{Field: field, Operator: op, Value: value}) {
							out <- r.id
						}
					}
					continue
				}
			}

			secRows, _, err := t.readSectionRows(sec)
			if err != nil {
				continue
			}
			for _, r := range bySection[sec] {
				if int(r.loc.Offset) >= len(secRows) {
					continue
				}
				row := secRows[int(r.loc.Offset)]
				fieldVal := tpath.PathLookup(row, field)
				if bfilters.ApplyFilterCondition(fieldVal, &query.FieldFilter{Field: field, Operator: op, Value: value}) {
					out <- r.id
				}
			}
		}
	}()
	return out
}

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
	golog.Printf("[AddRows] table=%s rows=%d acquiring lock...", t.name, len(elems))
	t.lock.Lock()
	defer t.lock.Unlock()

	golog.Printf("[AddRows] table=%s reserveSection...", t.name)
	section, err := t.reserveSection()
	if err != nil {
		return nil, err
	}
	golog.Printf("[AddRows] table=%s writeSection section=%d rows=%d...", t.name, section, len(elems))
	if err := t.writeSection(section, elems); err != nil {
		return nil, err
	}

	golog.Printf("[AddRows] table=%s indexDB.Update section=%d rows=%d...", t.name, section, len(elems))
	locs := make([]*benchtop.RowLoc, len(elems))
	err = t.indexDB.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(idsBucket))
		fwd := tx.Bucket([]byte(fieldIndexBucket))
		rev := tx.Bucket([]byte(reverseFieldIndexBucket))
		if b == nil {
			return fmt.Errorf("missing ids bucket")
		}
		indexed := make([]string, 0, len(t.indexedFields))
		for f := range t.indexedFields {
			indexed = append(indexed, f)
		}
		for i, row := range elems {
			loc := &benchtop.RowLoc{TableId: t.tableID, Section: section, Offset: uint32(i), Size: 0, Index: 0}
			if err := b.Put(row.Id, encodeRowLoc(loc)); err != nil {
				return err
			}
			if len(indexed) > 0 && fwd != nil && rev != nil {
				rowID := string(row.Id)
				for _, field := range indexed {
					fieldVal := tpath.PathLookup(row.Data, field)
					if fieldVal == nil {
						continue
					}
					valueBytes, err := sonic.ConfigFastest.Marshal(fieldVal)
					if err != nil {
						continue
					}
					if err := fwd.Put(makeFieldIndexKey(field, valueBytes, rowID), nil); err != nil {
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

	golog.Printf("[AddRows] table=%s section=%d DONE", t.name, section)
	return locs, nil
}

func (t *ArrowTable) ScanDoc(filter benchtop.RowFilter) chan map[string]any {
	out := make(chan map[string]any, 100)
	go func() {
		defer close(out)
		filterActive := filter != nil && !filter.IsNoOp()
		if !filterActive {
			active, err := t.activeRowLocs()
			if err != nil {
				return
			}
			next, err := t.nextSection()
			if err != nil {
				return
			}
			for sec := uint16(0); sec < next; sec++ {
				err := t.streamSectionRows(sec, func(id string, row map[string]any, offset uint32) bool {
					loc, ok := active[id]
					if !ok {
						return true
					}
					if loc.Section != sec || loc.Offset != offset {
						return true
					}
					out <- row
					return true
				})
				if err != nil {
					continue
				}
			}
			return
		}
		rows, err := t.listIndexRows()
		if err != nil {
			return
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
			for _, r := range bySection[sec] {
				if int(r.loc.Offset) >= len(secRows) {
					continue
				}
				row := secRows[int(r.loc.Offset)]
				if filterActive {
					payload, err := sonic.ConfigFastest.Marshal(row)
					if err != nil {
						continue
					}
					if !filter.Matches(payload, t.name) {
						continue
					}
				}
				out <- row
			}
		}
	}()
	return out
}

func (t *ArrowTable) ScanDocProjected(fields []string, filter benchtop.RowFilter) chan map[string]any {
	out := make(chan map[string]any, 100)
	go func() {
		defer close(out)

		if len(fields) == 0 {
			for row := range t.ScanDoc(filter) {
				out <- row
			}
			return
		}

		filterActive := filter != nil && !filter.IsNoOp()
		needsJSON := filterActive
		for _, f := range fields {
			if strings.Contains(f, ".") || strings.Contains(f, "[") {
				needsJSON = true
				break
			}
		}
		if !filterActive && len(fields) > 0 {
			active, err := t.activeRowLocs()
			if err != nil {
				return
			}
			next, err := t.nextSection()
			if err != nil {
				return
			}
			for sec := uint16(0); sec < next; sec++ {
				if !needsJSON {
					projRows, err := t.readSectionProjectedRows(sec, fields)
					if err == nil {
						for i, row := range projRows {
							id, _ := row["_id"].(string)
							loc, ok := active[id]
							if !ok {
								continue
							}
							if loc.Section != sec || loc.Offset != uint32(i) {
								continue
							}
							out <- row
						}
						continue
					}
				}
				err := t.streamSectionRows(sec, func(id string, full map[string]any, offset uint32) bool {
					loc, ok := active[id]
					if !ok {
						return true
					}
					if loc.Section != sec || loc.Offset != offset {
						return true
					}
					proj := map[string]any{"_id": full["_id"]}
					for _, f := range fields {
						if f == "_id" {
							continue
						}
						if v, ok := full[f]; ok {
							proj[f] = v
						}
					}
					out <- proj
					return true
				})
				if err != nil {
					continue
				}
			}
			return
		}
		rows, err := t.listIndexRows()
		if err != nil {
			return
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
			if !needsJSON {
				projRows, err := t.readSectionProjectedRows(sec, fields)
				if err == nil {
					for _, r := range bySection[sec] {
						if int(r.loc.Offset) >= len(projRows) {
							continue
						}
						out <- projRows[int(r.loc.Offset)]
					}
					continue
				}
			}

			secRows, _, err := t.readSectionRows(sec)
			if err != nil {
				continue
			}
			for _, r := range bySection[sec] {
				if int(r.loc.Offset) >= len(secRows) {
					continue
				}
				full := secRows[int(r.loc.Offset)]
				if filterActive {
					payload, err := sonic.ConfigFastest.Marshal(full)
					if err != nil || !filter.Matches(payload, t.name) {
						continue
					}
				}
				proj := map[string]any{"_id": full["_id"]}
				for _, f := range fields {
					if f == "_id" {
						continue
					}
					if v, ok := full[f]; ok {
						proj[f] = v
					}
				}
				out <- proj
			}
		}
	}()
	return out
}

func (t *ArrowTable) ScanId(filter benchtop.RowFilter) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		if filter == nil || filter.IsNoOp() {
			// Fast path used by label scans: stream ids directly from index without
			// reading/decompressing Arrow row payloads.
			rows, err := t.listIndexRows()
			if err != nil {
				return
			}
			for _, r := range rows {
				out <- r.id
			}
			return
		}
		for row := range t.ScanDoc(filter) {
			if id, ok := row[idColumn].(string); ok {
				out <- id
			}
		}
	}()
	return out
}

func (t *ArrowTable) ScanFull(filter benchtop.RowFilter) chan benchtop.RowLocData {
	out := make(chan benchtop.RowLocData, 100)
	go func() {
		defer close(out)
		filterActive := filter != nil && !filter.IsNoOp()
		rows, err := t.listIndexRows()
		if err != nil {
			return
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
			for _, r := range bySection[sec] {
				if int(r.loc.Offset) >= len(secRows) {
					continue
				}
				row := secRows[int(r.loc.Offset)]
				if filterActive {
					payload, err := sonic.ConfigFastest.Marshal(row)
					if err != nil || !filter.Matches(payload, t.name) {
						continue
					}
				}
				out <- benchtop.RowLocData{DataMap: row, Loc: r.loc}
			}
		}
	}()
	return out
}

func (t *ArrowTable) DeleteRow(loc *benchtop.RowLoc, id []byte) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if len(id) == 0 {
		return nil
	}
	idStr := string(id)
	return t.indexDB.Update(func(tx *bbolt.Tx) error {
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

	rows, _, err := t.readSectionRows(loc.Section)
	if err != nil {
		return nil, err
	}
	if int(loc.Offset) >= len(rows) {
		return nil, fmt.Errorf("row not found at section=%d offset=%d", loc.Section, loc.Offset)
	}
	return rows[int(loc.Offset)], nil
}

func (t *ArrowTable) GetRows(locs []*benchtop.RowLoc, section uint16) ([]map[string]any, []error) {
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

	for sec, idxs := range bySection {
		rows, _, err := t.readSectionRows(sec)
		if err != nil {
			for _, i := range idxs {
				errs[i] = err
			}
			continue
		}
		for _, i := range idxs {
			loc := locs[i]
			if int(loc.Offset) >= len(rows) {
				errs[i] = fmt.Errorf("row not found at section=%d offset=%d", loc.Section, loc.Offset)
				continue
			}
			results[i] = rows[int(loc.Offset)]
		}
	}

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
		_ = os.Remove(t.sectionPath(sec))
	}

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
