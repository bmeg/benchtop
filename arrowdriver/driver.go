package arrowdriver

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/query"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"go.etcd.io/bbolt"
)

const driverMetaName = "driver.meta"
const bulkLoadBatchRows = 20000
const postBulkCompactRowsPerSection = 200000
const schemaGraphSuffix = "__schema__"

var (
	bucketTablesByName = []byte("tables_by_name")
	bucketNamesByID    = []byte("names_by_id")
	keyNextTableID     = []byte("next_table_id")
)

type ArrowDriver struct {
	base    string
	zoneDir string

	lock                sync.RWMutex
	tables              map[string]*ArrowTable
	tableIDs            map[string]uint16
	idToTable           map[uint16]string
	fields              map[string]map[string]struct{}
	schemaHints         map[string]tableWriteHints
	schemaHintsLoaded   bool
	schemaHintsBuilding bool
	metaDB              *bbolt.DB
}

type tableWriteHints struct {
	keys []string
	enc  map[string]columnEncoding
}

func NewArrowDriver(path string) (benchtop.TableDriver, error) {
	zoneDir := filepath.Join(path, "ARROW_TABLES")
	if err := os.MkdirAll(zoneDir, 0700); err != nil {
		return nil, err
	}

	metaPath := filepath.Join(zoneDir, driverMetaName)
	metaDB, err := bbolt.Open(metaPath, 0600, nil)
	if err != nil {
		return nil, err
	}

	d := &ArrowDriver{
		base:        path,
		zoneDir:     zoneDir,
		lock:        sync.RWMutex{},
		tables:      make(map[string]*ArrowTable),
		tableIDs:    make(map[string]uint16),
		idToTable:   make(map[uint16]string),
		fields:      make(map[string]map[string]struct{}),
		schemaHints: make(map[string]tableWriteHints),
		metaDB:      metaDB,
	}

	if err := d.initMeta(); err != nil {
		metaDB.Close()
		return nil, err
	}
	if err := d.discoverTables(); err != nil {
		metaDB.Close()
		return nil, err
	}
	return d, nil
}

func (d *ArrowDriver) initMeta() error {
	return d.metaDB.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketTablesByName); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketNamesByID); err != nil {
			return err
		}
		meta, err := tx.CreateBucketIfNotExists([]byte("meta"))
		if err != nil {
			return err
		}
		if meta.Get(keyNextTableID) == nil {
			v := make([]byte, 2)
			binary.LittleEndian.PutUint16(v, 1)
			if err := meta.Put(keyNextTableID, v); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *ArrowDriver) reserveTableID() (uint16, error) {
	var next uint16
	err := d.metaDB.Update(func(tx *bbolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
		if meta == nil {
			return fmt.Errorf("missing driver meta bucket")
		}
		v := meta.Get(keyNextTableID)
		if len(v) < 2 {
			next = 1
		} else {
			next = binary.LittleEndian.Uint16(v)
			if next == 0 {
				next = 1
			}
		}
		nv := make([]byte, 2)
		binary.LittleEndian.PutUint16(nv, next+1)
		return meta.Put(keyNextTableID, nv)
	})
	return next, err
}

func (d *ArrowDriver) setTableMeta(name string, tableID uint16) error {
	return d.metaDB.Update(func(tx *bbolt.Tx) error {
		byName := tx.Bucket(bucketTablesByName)
		byID := tx.Bucket(bucketNamesByID)
		if byName == nil || byID == nil {
			return fmt.Errorf("missing table metadata buckets")
		}
		idBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(idBytes, tableID)
		if err := byName.Put([]byte(name), idBytes); err != nil {
			return err
		}
		if err := byID.Put(idBytes, []byte(name)); err != nil {
			return err
		}
		return nil
	})
}

func (d *ArrowDriver) loadTableMeta(name string) (uint16, bool, error) {
	var (
		tableID uint16
		found   bool
	)
	err := d.metaDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketTablesByName)
		if b == nil {
			return nil
		}
		v := b.Get([]byte(name))
		if len(v) >= 2 {
			tableID = binary.LittleEndian.Uint16(v)
			found = true
		}
		return nil
	})
	return tableID, found, err
}

func (d *ArrowDriver) discoverTables() error {
	pattern := filepath.Join(d.zoneDir, "*"+indexFileExt)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	for _, p := range files {
		name := strings.TrimSuffix(filepath.Base(p), indexFileExt)
		if name == "" || name == driverMetaName {
			continue
		}
		if name == strings.TrimSuffix(driverMetaName, indexFileExt) {
			continue
		}

		tableID, found, err := d.loadTableMeta(name)
		if err != nil {
			return err
		}
		if !found {
			t, err := loadArrowTable(d.zoneDir, name)
			if err != nil {
				continue
			}
			tableID = t.TableID()
			if tableID == 0 {
				tableID, err = d.reserveTableID()
				if err != nil {
					t.Close()
					return err
				}
				if err := t.SetTableID(tableID); err != nil {
					t.Close()
					return err
				}
			}
			if err := d.setTableMeta(name, tableID); err != nil {
				t.Close()
				return err
			}
			t.Close()
		}

		d.tableIDs[name] = tableID
		d.idToTable[tableID] = name
		if _, ok := d.fields[name]; !ok {
			d.fields[name] = map[string]struct{}{}
		}
	}
	return nil
}

func (d *ArrowDriver) GetKV() any {
	return d.base
}

func (d *ArrowDriver) Close() {
	d.lock.Lock()
	defer d.lock.Unlock()
	for _, t := range d.tables {
		t.Close()
	}
	if d.metaDB != nil {
		d.metaDB.Close()
	}
}

// resolveTableName returns the canonical name for a table, handling
// case-insensitive filesystem collisions (e.g. macOS HFS+/APFS).
// If a case-variant of name already exists in tableIDs, the existing
// name is returned so that we reuse the same bbolt file instead of
// trying to open it a second time (which would deadlock on flock).
func (d *ArrowDriver) resolveTableName(name string) string {
	if _, ok := d.tableIDs[name]; ok {
		return name
	}
	lower := strings.ToLower(name)
	for existing := range d.tableIDs {
		if strings.ToLower(existing) == lower {
			return existing
		}
	}
	return name
}

func (d *ArrowDriver) New(name string, columns []benchtop.ColumnDef) (benchtop.TableStore, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	// Resolve case-variant names to prevent bbolt file lock deadlocks
	// on case-insensitive filesystems.
	name = d.resolveTableName(name)

	if t, ok := d.tables[name]; ok {
		log.Debugf("arrowdriver.New reuse_open_table name=%s", name)
		return t, nil
	}
	if _, ok := d.tableIDs[name]; ok {
		log.Debugf("arrowdriver.New load_existing_table name=%s", name)
		return d.getOrLoadLocked(name)
	}

	tableID, err := d.reserveTableID()
	if err != nil {
		return nil, err
	}
	t, err := newArrowTable(d.zoneDir, name, tableID, columns)
	if err != nil {
		return nil, err
	}
	if err := d.setTableMeta(name, tableID); err != nil {
		t.Close()
		return nil, err
	}
	d.tables[name] = t
	d.applySchemaHintsLocked(name, t)
	d.tableIDs[name] = tableID
	d.idToTable[tableID] = name
	if _, ok := d.fields[name]; !ok {
		d.fields[name] = make(map[string]struct{})
	}
	log.Infof("arrowdriver.New created_table name=%s tableID=%d columns=%d", name, tableID, len(columns))
	return t, nil
}

func (d *ArrowDriver) getOrLoadLocked(name string) (*ArrowTable, error) {
	if t, ok := d.tables[name]; ok {
		return t, nil
	}
	start := time.Now()
	t, err := loadArrowTable(d.zoneDir, name)
	if err != nil {
		return nil, err
	}
	if tableID, ok := d.tableIDs[name]; ok && tableID > 0 && t.TableID() != tableID {
		if err := t.SetTableID(tableID); err != nil {
			t.Close()
			return nil, err
		}
	}
	d.tables[name] = t
	if t.TableID() > 0 {
		d.tableIDs[name] = t.TableID()
		d.idToTable[t.TableID()] = name
	}
	if _, ok := d.fields[name]; !ok {
		d.fields[name] = make(map[string]struct{})
	}
	for _, idxField := range t.IndexedFields() {
		d.fields[name][idxField] = struct{}{}
	}
	d.applySchemaHintsLocked(name, t)
	log.Debugf("arrowdriver.getOrLoad loaded_table name=%s tableID=%d indexedFields=%d elapsed=%s", name, t.TableID(), len(t.IndexedFields()), time.Since(start).Round(time.Millisecond))
	return t, nil
}

func (d *ArrowDriver) applySchemaHintsLocked(tableName string, t *ArrowTable) {
	if strings.HasSuffix(tableName, schemaGraphSuffix) || t == nil {
		return
	}
	if !d.schemaHintsLoaded && !d.schemaHintsBuilding {
		d.refreshSchemaHintsLocked()
	}
	if hint, ok := d.schemaHints[tableName]; ok && len(hint.keys) > 0 {
		// Keep schema hints additive so structural/runtime fields
		// (for example edge linkage fields) are not dropped.
		t.SetWriteHints(hint.keys, hint.enc, false)
		log.Infof("arrowdriver.schema_hints_applied table=%s hintedFields=%d", tableName, len(hint.keys))
	}
}

func (d *ArrowDriver) refreshSchemaHintsLocked() {
	if d.schemaHintsBuilding {
		return
	}
	d.schemaHintsBuilding = true
	defer func() {
		d.schemaHintsBuilding = false
		d.schemaHintsLoaded = true
	}()
	out := map[string]tableWriteHints{}
	for tableName := range d.tableIDs {
		if !strings.HasSuffix(tableName, schemaGraphSuffix) {
			continue
		}
		store, err := d.getOrLoadLocked(tableName)
		if err != nil {
			continue
		}
		for row := range store.ScanDoc(nil) {
			label, keys, enc, ok := extractWriteHintsFromSchemaRow(row)
			if !ok {
				continue
			}
			targets := []string{label, "v_" + label, "e_" + label}
			for _, target := range targets {
				merged := mergeWriteHints(out[target], keys, enc)
				out[target] = merged
			}
		}
	}
	d.schemaHints = out
}

func mergeWriteHints(cur tableWriteHints, keys []string, enc map[string]columnEncoding) tableWriteHints {
	keySet := map[string]struct{}{}
	for _, k := range cur.keys {
		keySet[k] = struct{}{}
	}
	for _, k := range keys {
		if k == "" || k == idColumn || k == dataColumn {
			continue
		}
		if _, ok := keySet[k]; ok {
			continue
		}
		keySet[k] = struct{}{}
		cur.keys = append(cur.keys, k)
	}
	sort.Strings(cur.keys)
	if cur.enc == nil {
		cur.enc = map[string]columnEncoding{}
	}
	for k, v := range enc {
		if _, ok := keySet[k]; ok {
			cur.enc[k] = v
		}
	}
	return cur
}

func extractWriteHintsFromSchemaRow(row map[string]any) (string, []string, map[string]columnEncoding, bool) {
	src := row
	if v, ok := row["vertex"].(map[string]any); ok {
		src = v
	} else if v, ok := row["vertex"].(string); ok && v != "" {
		tmp := map[string]any{}
		if err := sonic.ConfigFastest.Unmarshal([]byte(v), &tmp); err == nil && len(tmp) > 0 {
			src = tmp
		}
	} else if v, ok := row["vertex"].([]byte); ok && len(v) > 0 {
		tmp := map[string]any{}
		if err := sonic.ConfigFastest.Unmarshal(v, &tmp); err == nil && len(tmp) > 0 {
			src = tmp
		}
	}
	label := schemaLabelFromMap(src)
	if label == "" {
		label = schemaLabelFromMap(row)
	}
	if label == "" {
		return "", nil, nil, false
	}

	keys := []string{}
	enc := map[string]columnEncoding{}
	if props, ok := src["properties"].(map[string]any); ok {
		for field, def := range props {
			if field == "" || field == idColumn || field == dataColumn {
				continue
			}
			keys = append(keys, field)
			enc[field] = schemaTypeToEncoding(def)
		}
	} else {
		for field, def := range src {
			if field == "" || strings.HasPrefix(field, "_") || field == "id" || field == "$id" || field == "label" || field == "title" {
				continue
			}
			keys = append(keys, field)
			enc[field] = schemaTypeToEncoding(def)
		}
	}
	if len(keys) == 0 {
		return "", nil, nil, false
	}
	sort.Strings(keys)
	return label, keys, enc, true
}

func schemaLabelFromMap(m map[string]any) string {
	for _, k := range []string{"_label", "label", "title", "name"} {
		if s, ok := m[k].(string); ok && s != "" {
			return normalizeSchemaLabel(s)
		}
	}
	if s, ok := m["_id"].(string); ok && s != "" {
		return normalizeSchemaLabel(s)
	}
	if s, ok := m["id"].(string); ok && s != "" {
		return normalizeSchemaLabel(s)
	}
	return ""
}

func normalizeSchemaLabel(s string) string {
	if i := strings.LastIndex(s, "/"); i >= 0 && i+1 < len(s) {
		s = s[i+1:]
	}
	if strings.HasPrefix(s, "v_") || strings.HasPrefix(s, "e_") {
		return s[2:]
	}
	return s
}

func schemaTypeToEncoding(v any) columnEncoding {
	switch tv := v.(type) {
	case string:
		switch strings.ToLower(tv) {
		case "string":
			return encString
		case "boolean", "bool":
			return encBool
		case "number", "integer", "float", "double", "long", "int":
			return encFloat64
		default:
			return encJSON
		}
	case map[string]any:
		if t, ok := tv["type"]; ok {
			return schemaTypeToEncoding(t)
		}
		return encJSON
	case []any:
		// JSON schema can expose union types in arrays, prefer scalar when present.
		for _, e := range tv {
			enc := schemaTypeToEncoding(e)
			if enc != encJSON {
				return enc
			}
		}
		return encJSON
	default:
		return encJSON
	}
}

func (d *ArrowDriver) Get(tableID uint16) (benchtop.TableStore, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	name, ok := d.idToTable[tableID]
	if !ok {
		return nil, fmt.Errorf("table ID %d not found", tableID)
	}

	return d.getOrLoadLocked(name)
}

func (d *ArrowDriver) List() []string {
	d.lock.RLock()
	defer d.lock.RUnlock()
	out := make([]string, 0, len(d.tableIDs))
	for name := range d.tableIDs {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func (d *ArrowDriver) BulkLoad(tableID uint16, rows chan *benchtop.Row) error {
	start := time.Now()
	tableStore, err := d.Get(tableID)
	if err != nil {
		log.Errorf("BulkLoad Get error: %v", err)
		return err
	}
	at, ok := tableStore.(*ArrowTable)
	if !ok {
		return fmt.Errorf("table ID %d is not ArrowTable", tableID)
	}
	log.Infof("arrowdriver.BulkLoad start table=%s tableID=%d batchSize=%d", at.name, tableID, bulkLoadBatchRows)

	batch := make([]benchtop.Row, 0, bulkLoadBatchRows)
	var totalRows int
	var flushes int
	for row := range rows {
		if row == nil {
			continue
		}
		batch = append(batch, *row)
		totalRows++
		if len(batch) >= bulkLoadBatchRows {
			flushStart := time.Now()
			if err := at.BulkLoad(batch); err != nil {
				return err
			}
			flushes++
			log.Debugf("arrowdriver.BulkLoad flush table=%s tableID=%d rows=%d flush=%d elapsed=%s", at.name, tableID, len(batch), flushes, time.Since(flushStart).Round(time.Millisecond))
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		flushStart := time.Now()
		if err := at.BulkLoad(batch); err != nil {
			return err
		}
		flushes++
		log.Debugf("arrowdriver.BulkLoad flush table=%s tableID=%d rows=%d flush=%d elapsed=%s", at.name, tableID, len(batch), flushes, time.Since(flushStart).Round(time.Millisecond))
	}
	compactStart := time.Now()
	if err := at.CompactSections(postBulkCompactRowsPerSection); err != nil {
		log.Warningf("arrowdriver.BulkLoad compact_error table=%s tableID=%d err=%v", at.name, tableID, err)
	} else {
		log.Infof("arrowdriver.BulkLoad compact_done table=%s tableID=%d targetRowsPerSection=%d elapsed=%s", at.name, tableID, postBulkCompactRowsPerSection, time.Since(compactStart).Round(time.Millisecond))
	}
	log.Infof("arrowdriver.BulkLoad done table=%s tableID=%d rows=%d flushes=%d elapsed=%s", at.name, tableID, totalRows, flushes, time.Since(start).Round(time.Millisecond))
	return nil
}

func (d *ArrowDriver) RowIdsByHas(field string, value any, op query.Condition) chan benchtop.Index {
	out := make(chan benchtop.Index, 100)
	go func() {
		defer close(out)
		start := time.Now()
		total := 0
		for _, name := range d.List() {
			tableStore, err := d.Get(d.tableIDs[name]) // Changed to use tableID
			if err != nil {
				continue
			}
			table, ok := tableStore.(*ArrowTable)
			if !ok {
				continue
			}
			tableStart := time.Now()
			matched := 0
			for idx := range table.RowIndexesByHas(field, value, op) {
				out <- idx
				matched++
				total++
			}
			log.Debugf("arrowdriver.RowIdsByHas table=%s field=%s op=%d matched=%d elapsed=%s", name, field, op, matched, time.Since(tableStart).Round(time.Millisecond))
		}
		log.Debugf("arrowdriver.RowIdsByHas done field=%s op=%d total=%d elapsed=%s", field, op, total, time.Since(start).Round(time.Millisecond))
	}()
	return out
}

func (d *ArrowDriver) ListTableKeys(tableID uint16) (chan benchtop.Index, error) {
	d.lock.RLock()
	name, ok := d.idToTable[tableID]
	d.lock.RUnlock()
	if !ok {
		out := make(chan benchtop.Index)
		close(out)
		return out, nil
	}
	store, err := d.Get(tableID) // Changed to use tableID
	if err != nil {
		return nil, err
	}
	t, ok := store.(*ArrowTable)
	if !ok {
		return nil, fmt.Errorf("table %q is not ArrowTable", name)
	}
	return t.ListTableKeys()
}

func (d *ArrowDriver) GetAllColNames() chan string {
	out := make(chan string, 10)
	go func() {
		defer close(out)
		seen := make(map[string]struct{})
		for _, name := range d.List() {
			tableStore, err := d.Get(d.tableIDs[name]) // Changed to use tableID
			if err != nil {
				continue
			}
			for _, col := range tableStore.GetColumnDefs() {
				if _, ok := seen[col.Key]; ok {
					continue
				}
				seen[col.Key] = struct{}{}
				out <- col.Key
			}
		}
	}()
	return out
}

func (d *ArrowDriver) GetLabels(edges bool, removePrefix bool) chan string {
	out := make(chan string, 10)
	go func() {
		defer close(out)
		for _, label := range d.List() {
			isEdge := strings.HasPrefix(label, "e_")
			if (edges && isEdge) || (!edges && !isEdge) {
				if removePrefix && len(label) > 2 {
					out <- label[2:]
				} else {
					out <- label
				}
			}
		}
	}()
	return out
}

func (d *ArrowDriver) RowIdsByTableFieldValue(tableID uint16, field string, value any, op query.Condition) chan benchtop.Index {
	out := make(chan benchtop.Index, 100)
	go func() {
		defer close(out)
		store, err := d.Get(tableID)
		if err != nil {
			return
		}
		table, ok := store.(*ArrowTable)
		if !ok {
			return
		}
		for idx := range table.RowIndexesByHas(field, value, op) {
			out <- idx
		}
	}()
	return out
}

func (d *ArrowDriver) Delete(tableID uint16) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	name, ok := d.idToTable[tableID]
	if !ok {
		return fmt.Errorf("table ID %d not found", tableID)
	}

	if t, ok := d.tables[name]; ok {
		t.Close()
		delete(d.tables, name)
	}
	delete(d.tableIDs, name)
	delete(d.fields, name)
	delete(d.idToTable, tableID)

	_ = d.metaDB.Update(func(tx *bbolt.Tx) error {
		if b := tx.Bucket(bucketTablesByName); b != nil {
			_ = b.Delete([]byte(name))
		}
		if tableID > 0 {
			idBytes := make([]byte, 2)
			binary.LittleEndian.PutUint16(idBytes, tableID)
			if b := tx.Bucket(bucketNamesByID); b != nil {
				_ = b.Delete(idBytes)
			}
		}
		return nil
	})

	_ = os.Remove(filepath.Join(d.zoneDir, name+indexFileExt))
	pattern := filepath.Join(d.zoneDir, fmt.Sprintf("%s_*%s", name, arrowFileExt))
	segments, _ := filepath.Glob(pattern)
	for _, seg := range segments {
		_ = os.Remove(seg)
	}
	return nil
}

func (d *ArrowDriver) LookupTableID(name string) (uint16, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	name = d.resolveTableName(name)
	if id, ok := d.tableIDs[name]; ok {
		return id, nil
	}
	return 0, fmt.Errorf("table %q not found", name)
}

func (d *ArrowDriver) ListTableIDs() []uint16 {
	d.lock.RLock()
	defer d.lock.RUnlock()
	ids := make([]uint16, 0, len(d.idToTable))
	for id := range d.idToTable {
		ids = append(ids, id)
	}
	return ids
}

func (d *ArrowDriver) GetTableInfo(tableID uint16) (*benchtop.TableInfo, error) {
	d.lock.RLock()
	name, ok := d.idToTable[tableID]
	d.lock.RUnlock()
	if !ok {
		return nil, fmt.Errorf("table ID %d not found", tableID)
	}
	return &benchtop.TableInfo{
		Name:    name,
		TableId: tableID,
	}, nil
}

func (d *ArrowDriver) AddField(tableID uint16, field string) error {
	d.lock.Lock()
	name, ok := d.idToTable[tableID]
	if !ok {
		d.lock.Unlock()
		return fmt.Errorf("table ID %d not found", tableID)
	}
	t, err := d.getOrLoadLocked(name)
	if err != nil {
		d.lock.Unlock()
		return err
	}
	if _, ok := d.fields[name]; !ok {
		d.fields[name] = map[string]struct{}{}
	}
	d.fields[name][field] = struct{}{}
	d.lock.Unlock()
	start := time.Now()
	log.Infof("arrowdriver.AddField ensure_index_start table=%s tableID=%d field=%s", name, tableID, field)
	err = t.EnsureFieldIndex(field)
	if err != nil {
		log.Errorf("arrowdriver.AddField ensure_index_error table=%s tableID=%d field=%s err=%v", name, tableID, field, err)
		return err
	}
	log.Infof("arrowdriver.AddField ensure_index_done table=%s tableID=%d field=%s elapsed=%s", name, tableID, field, time.Since(start).Round(time.Millisecond))
	return nil
}

func (d *ArrowDriver) RemoveField(tableID uint16, field string) error {
	d.lock.Lock()
	name, ok := d.idToTable[tableID]
	if !ok {
		d.lock.Unlock()
		return fmt.Errorf("table ID %d not found", tableID)
	}
	t, err := d.getOrLoadLocked(name)
	if err != nil {
		d.lock.Unlock()
		return err
	}
	if fields, ok := d.fields[name]; ok {
		delete(fields, field)
	}
	d.lock.Unlock()
	start := time.Now()
	log.Infof("arrowdriver.RemoveField remove_index_start table=%s tableID=%d field=%s", name, tableID, field)
	err = t.RemoveFieldIndex(field)
	if err != nil {
		log.Errorf("arrowdriver.RemoveField remove_index_error table=%s tableID=%d field=%s err=%v", name, tableID, field, err)
		return err
	}
	log.Infof("arrowdriver.RemoveField remove_index_done table=%s tableID=%d field=%s elapsed=%s", name, tableID, field, time.Since(start).Round(time.Millisecond))
	return nil
}

func (d *ArrowDriver) ListFields() []benchtop.FieldInfo {
	d.lock.RLock()
	defer d.lock.RUnlock()

	out := make([]benchtop.FieldInfo, 0)
	for label, fields := range d.fields {
		for field := range fields {
			out = append(out, benchtop.FieldInfo{Label: label, Field: field})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Label == out[j].Label {
			return out[i].Field < out[j].Field
		}
		return out[i].Label < out[j].Label
	})
	return out
}

func (d *ArrowDriver) DeleteRowField(tableID uint16, field, rowID string) error {
	// Arrow driver computes field filters from row payloads at query time.
	// There is no separate field-index keyspace to mutate for one row.
	return nil
}

func (d *ArrowDriver) InvalidateLoc(tableID uint16, rowID string) {
	// Arrow driver does not currently use a table-aware location cache
}

func (d *ArrowDriver) GetIDsForLabel(label string) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		id, err := d.LookupTableID(label)
		if err != nil {
			return
		}
		store, err := d.Get(id)
		if err != nil {
			return
		}
		for id := range store.ScanId(nil) {
			out <- id
		}
	}()
	return out
}
