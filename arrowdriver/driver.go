package arrowdriver

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/query"
	"github.com/bmeg/grip/log"
	"go.etcd.io/bbolt"
)

const driverMetaName = "driver.meta"
const bulkLoadBatchRows = 20000

var (
	bucketTablesByName = []byte("tables_by_name")
	bucketNamesByID    = []byte("names_by_id")
	keyNextTableID     = []byte("next_table_id")
)

type ArrowDriver struct {
	base    string
	zoneDir string

	lock      sync.RWMutex
	tables    map[string]*ArrowTable
	tableIDs  map[string]uint16
	idToTable map[uint16]string
	fields    map[string]map[string]struct{}
	metaDB    *bbolt.DB
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
		base:      path,
		zoneDir:   zoneDir,
		lock:      sync.RWMutex{},
		tables:    make(map[string]*ArrowTable),
		tableIDs:  make(map[string]uint16),
		idToTable: make(map[uint16]string),
		fields:    make(map[string]map[string]struct{}),
		metaDB:    metaDB,
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
		return t, nil
	}
	if _, ok := d.tableIDs[name]; ok {
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
	d.tableIDs[name] = tableID
	d.idToTable[tableID] = name
	if _, ok := d.fields[name]; !ok {
		d.fields[name] = make(map[string]struct{})
	}
	return t, nil
}

func (d *ArrowDriver) getOrLoadLocked(name string) (*ArrowTable, error) {
	if t, ok := d.tables[name]; ok {
		return t, nil
	}
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
	return t, nil
}

func (d *ArrowDriver) Get(name string) (benchtop.TableStore, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	// Resolve case-variant names.
	name = d.resolveTableName(name)

	if _, ok := d.tableIDs[name]; !ok {
		return nil, fmt.Errorf("table %q not found", name)
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

func (d *ArrowDriver) BulkLoad(name string, rows chan benchtop.Row) error {
	tableStore, err := d.Get(name)
	if err != nil {
		log.Errorf("BulkLoad Get error: %v", err)
		return err
	}
	at, ok := tableStore.(*ArrowTable)
	if !ok {
		return fmt.Errorf("table %q is not ArrowTable", name)
	}

	batch := make([]benchtop.Row, 0, bulkLoadBatchRows)
	for row := range rows {
		batch = append(batch, row)
		if len(batch) >= bulkLoadBatchRows {
			if err := at.BulkLoad(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := at.BulkLoad(batch); err != nil {
			return err
		}
	}
	return nil
}

func (d *ArrowDriver) RowIdsByHas(field string, value any, op query.Condition) chan benchtop.Index {
	out := make(chan benchtop.Index, 100)
	go func() {
		defer close(out)
		for _, name := range d.List() {
			tableStore, err := d.Get(name)
			if err != nil {
				continue
			}
			table, ok := tableStore.(*ArrowTable)
			if !ok {
				continue
			}
			for id := range table.RowIdsByHas(field, value, op) {
				loc, err := table.GetRowLoc(id)
				if err != nil {
					continue
				}
				out <- benchtop.Index{Key: []byte(id), Loc: loc}
			}
		}
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
	store, err := d.Get(name)
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
			tableStore, err := d.Get(name)
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

func (d *ArrowDriver) RowIdsByLabelFieldValue(label, field string, value any, op query.Condition) chan benchtop.Index {
	out := make(chan benchtop.Index, 100)
	go func() {
		defer close(out)
		store, err := d.Get(label)
		if err != nil {
			return
		}
		table, ok := store.(*ArrowTable)
		if !ok {
			return
		}
		for id := range table.RowIdsByHas(field, value, op) {
			loc, err := table.GetRowLoc(id)
			if err != nil {
				continue
			}
			out <- benchtop.Index{Key: []byte(id), Loc: loc}
		}
	}()
	return out
}

func (d *ArrowDriver) Delete(name string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	if t, ok := d.tables[name]; ok {
		t.Close()
		delete(d.tables, name)
	}
	tableID := d.tableIDs[name]
	delete(d.tableIDs, name)
	delete(d.fields, name)
	if tableID > 0 {
		delete(d.idToTable, tableID)
	}

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

func (d *ArrowDriver) AddField(label, field string) error {
	d.lock.Lock()
	name := d.resolveTableName(label)
	if _, ok := d.tableIDs[name]; !ok {
		tableID, err := d.reserveTableID()
		if err != nil {
			d.lock.Unlock()
			return err
		}
		t, err := newArrowTable(d.zoneDir, name, tableID, nil)
		if err != nil {
			d.lock.Unlock()
			return err
		}
		if err := d.setTableMeta(name, tableID); err != nil {
			t.Close()
			d.lock.Unlock()
			return err
		}
		d.tables[name] = t
		d.tableIDs[name] = tableID
		d.idToTable[tableID] = name
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
	return t.EnsureFieldIndex(field)
}

func (d *ArrowDriver) RemoveField(label, field string) error {
	d.lock.Lock()
	name := d.resolveTableName(label)
	t, err := d.getOrLoadLocked(name)
	if err != nil {
		d.lock.Unlock()
		return err
	}
	if fields, ok := d.fields[name]; ok {
		delete(fields, field)
	}
	d.lock.Unlock()
	return t.RemoveFieldIndex(field)
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

func (d *ArrowDriver) DeleteRowField(label, field, rowID string) error {
	// Arrow driver computes field filters from row payloads at query time.
	// There is no separate field-index keyspace to mutate for one row.
	return nil
}

func (d *ArrowDriver) GetIDsForLabel(label string) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		store, err := d.Get(label)
		if err != nil {
			return
		}
		for id := range store.ScanId(nil) {
			out <- id
		}
	}()
	return out
}
