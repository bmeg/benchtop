package jsontable

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"strconv"
	"strings"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/cache"
	"github.com/bmeg/benchtop/jsontable/block"
	"github.com/bmeg/benchtop/jsontable/storage"
	"github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/benchtop/util"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
	"github.com/maypok86/otter/v2"
)

const (
	BATCH_SIZE int = 5000
)

type JSONDriver struct {
	base       string
	Lock       sync.RWMutex
	PebbleLock sync.RWMutex
	Pkv        *pebblebulk.PebbleKV
	LocCache   cache.Cache

	Tables      map[uint16]*table.JSONTable
	idToName    map[uint16]string
	nameToId    map[string]uint16
	ZoneManager storage.ZoneManager
}

func NewJSONDriver(path string) (benchtop.TableDriver, error) {
	pKv, err := pebblebulk.NewPebbleKV(path)
	if err != nil {
		return nil, err
	}
	tableDir := filepath.Join(path, "TABLES")
	exist, err := util.DirExists(tableDir)
	if err != nil {
		return nil, err
	}
	if !exist {
		if err := os.Mkdir(tableDir, 0700); err != nil {
			pKv.Db.Close()
			return nil, fmt.Errorf("failed to create TABLES directory: %v", err)
		}
	}

	driver := &JSONDriver{
		base:        path,
		Tables:      map[uint16]*table.JSONTable{},
		Pkv:         pKv,
		LocCache:    cache.NewKVCache(pKv),
		Lock:        sync.RWMutex{},
		PebbleLock:  sync.RWMutex{},
		idToName:    map[uint16]string{},
		nameToId:    map[string]uint16{},
		ZoneManager: storage.NewZoneManager(tableDir),
	}

	// Load existing tables from disk
	for _, tableName := range driver.List() {
		tinfo, err := driver.getTableInfo(tableName)
		if err != nil {
			driver.Close()
			return nil, fmt.Errorf("failed to load table %s: %v", tableName, err)
		}
		driver.nameToId[tableName] = tinfo.TableId
		driver.idToName[tinfo.TableId] = tableName

		_, err = driver.Get(tinfo.TableId)
		if err != nil {
			driver.Close()
			return nil, fmt.Errorf("failed to open table %s (ID %d): %v", tableName, tinfo.TableId, err)
		}
	}

	// Load Fields
	if err := driver.LoadFields(); err != nil {
		driver.Close()
		return nil, fmt.Errorf("failed to load fields: %v", err)
	}

	// Preload Cache
	// Note: cache.NewKVCache already handles table-aware scanning if we updated it.
	driver.Lock.RLock()
	err = driver.LocCache.PreloadCache()
	driver.Lock.RUnlock()
	if err != nil {
		log.Errorf("Cache preload failed: %v", err)
	}

	return driver, nil
}

// makeLocCacheKey creates a unique key for the location cache including tableId
func makeLocCacheKey(tableId uint16, id string) string {
	return strconv.FormatUint(uint64(tableId), 10) + ":" + id
}

// LoadJSONDriver is deprecated and just calls NewJSONDriver which now handles loading.
func LoadJSONDriver(path string) (benchtop.TableDriver, error) {
	return NewJSONDriver(path)
}

func (dr *JSONDriver) New(name string, columns []benchtop.ColumnDef) (benchtop.TableStore, error) {
	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	if id, ok := dr.nameToId[name]; ok {
		if p, ok := dr.Tables[id]; ok {
			return p, nil
		}
		// Attempt to load if we know the ID but it's not in Tables map
		// Release lock before calling Get to avoid deadlock (Get acquires ReadLock/Lock)
		dr.Lock.Unlock()
		tbl, err := dr.Get(id)
		dr.Lock.Lock() // Re-acquire lock
		if err == nil {
			return tbl, nil
		}
	}

	// Case-insensitive lookup for existing tables on startup
	lowerName := strings.ToLower(name)
	for existingName, id := range dr.nameToId {
		if strings.ToLower(existingName) == lowerName {
			if p, ok := dr.Tables[id]; ok {
				return p, nil
			}
		}
	}

	newId := dr.getMaxTablePrefix()
	formattedName := util.PadToSixDigits(int(newId))
	tPath := filepath.Join(dr.base, "TABLES", formattedName)

	out, err := dr.newJSONTable(name, columns, formattedName, newId)
	if err != nil {
		return nil, err
	}

	// Create TableInfo for serialization
	tinfo := &benchtop.TableInfo{
		Columns:  columns,
		TableId:  newId,
		Path:     tPath,
		FileName: formattedName,
		Name:     name,
	}

	outData, err := sonic.ConfigFastest.Marshal(tinfo)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal table info: %v", err)
	}

	if err := dr.addTable(tinfo.Name, outData); err != nil {
		log.Errorf("Error adding table: %s", err)
		return nil, err
	}

	if err := out.Init(10); err != nil {
		// Init might be no-op now
	}

	dr.Tables[newId] = out
	dr.nameToId[name] = newId
	dr.Tables[newId] = out
	dr.nameToId[name] = newId
	dr.idToName[newId] = name

	log.Debugf("Created table %s with ID %d", name, newId)
	return out, nil
}

func (dr *JSONDriver) SetIndices(inputs chan benchtop.Index) {
	dr.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		for index := range inputs {
			dr.AddTableEntryInfo(
				tx,
				index.Key,
				index.Loc,
			)
		}
		return nil
	})
}

func (dr *JSONDriver) ListTableKeys(tableId uint16) (chan benchtop.Index, error) {
	out := make(chan benchtop.Index, 10)
	go func() {
		defer close(out)
		prefix := benchtop.NewPosKeyPrefix(tableId)
		dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				_, value := benchtop.ParsePosKey(it.Key())
				out <- benchtop.Index{Key: value}
			}
			return nil
		})
	}()
	return out, nil
}

func (dr *JSONDriver) List() []string {
	out := []string{}
	prefix := []byte{benchtop.TablePrefix}
	dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			value := benchtop.ParseTableKey(it.Key())
			out = append(out, string(value))
		}
		return nil
	})
	return out
}

func (dr *JSONDriver) GetLabels(edges bool, removePrefix bool) chan string {
	out := make(chan string, 10)
	go func() {
		defer close(out)
		dr.Lock.RLock()
		defer dr.Lock.RUnlock()
		for _, name := range dr.idToName {
			isEdge := strings.HasPrefix(name, "e_")
			if (edges && isEdge) || (!edges && !isEdge) {
				if removePrefix && len(name) > 2 {
					out <- name[2:]
				} else {
					out <- name
				}
			}
		}
	}()
	return out
}

func (dr *JSONDriver) GetAllColNames() chan string {
	out := make(chan string, 10)
	go func() {
		defer close(out)
		dr.Lock.RLock()
		defer dr.Lock.RUnlock()
		for _, tbl := range dr.Tables {
			for _, col := range tbl.GetColumnDefs() {
				out <- col.Key
			}
		}
	}()
	return out
}

// BulkLoad implementation is in bLoad.go

func (dr *JSONDriver) GetKV() any {
	return dr.Pkv
}

func (dr *JSONDriver) Close() {
	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	log.Infoln("Closing JSONDriver...")
	for id, table := range dr.Tables {
		table.Close() // Closes all section handles and file pools
		log.Debugf("Closed table ID %d (%s)", id, table.Name)
	}
	dr.Tables = make(map[uint16]*table.JSONTable)
	dr.nameToId = make(map[string]uint16)
	if dr.Pkv != nil && dr.Pkv.Db != nil {
		if closeErr := dr.Pkv.Db.Close(); closeErr != nil {
			log.Errorf("Error closing Pebble database: %v", closeErr)
		}
		dr.Pkv.Db = nil
		time.Sleep(50 * time.Millisecond)
	}
	dr.Pkv = nil
	log.Infof("Successfully closed JSONDriver for path %s", dr.base)
}

func (dr *JSONDriver) InvalidateLoc(tableId uint16, rowId string) {
	dr.LocCache.Invalidate(makeLocCacheKey(tableId, rowId))
}

func (dr *JSONDriver) Get(id uint16) (benchtop.TableStore, error) {
	dr.Lock.RLock()
	if x, ok := dr.Tables[id]; ok {
		dr.Lock.RUnlock()
		return x, nil
	}
	dr.Lock.RUnlock()

	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	if x, ok := dr.Tables[id]; ok {
		return x, nil
	}

	// Find the name for this ID in idToName
	name, ok := dr.idToName[id]
	if !ok {
		return nil, fmt.Errorf("table ID %d not found", id)
	}

	tinfo, err := dr.getTableInfo(name)
	if err != nil {
		log.Errorf("JSONDriver Get(ID %d): could not find info for name '%s': %v", id, name, err)
		return nil, err
	}

	log.Debugf("Opening Table ID %d: %#v\n", id, tinfo)

	out, err := dr.newJSONTable(name, tinfo.Columns, string(tinfo.FileName), tinfo.TableId)
	if err != nil {
		return nil, err
	}

	if err := out.Init(10); err != nil {
		return nil, fmt.Errorf("failed to init table %s: %v", name, err)
	}

	dr.Tables[id] = out
	dr.nameToId[name] = id

	return out, nil
}

func (dr *JSONDriver) Delete(id uint16) error {
	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	tableVar, exists := dr.Tables[id]
	if !exists {
		// Attempt to load it first to ensure we can close and delete it
		tbl, err := dr.Get(id)
		if err != nil {
			return fmt.Errorf("table ID %d does not exist and could not be loaded", id)
		}
		tableVar = tbl.(*table.JSONTable)
	}

	tableVar.Close() // Close all section files
	name := tableVar.Name

	// Delete the entire storage zone (O(1) bulk delete)
	if err := dr.ZoneManager.DeleteZone(tableVar.FileName); err != nil {
		log.Errorf("Failed to delete storage zone for %s: %v", name, err)
	}

	// Iterate over keys to invalidate cache and delete from KV
	prefix := benchtop.NewPosKeyPrefix(tableVar.TableId)
	var keysToDelete [][]byte
	dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			_, rowBytes := benchtop.ParsePosKey(it.Key())
			rowId := string(rowBytes)
			dr.LocCache.Invalidate(makeLocCacheKey(tableVar.TableId, rowId))
			// Make a copy of the key bytes because pebble reuses them
			keyCopy := make([]byte, len(it.Key()))
			copy(keyCopy, it.Key())
			keysToDelete = append(keysToDelete, keyCopy)
		}
		return nil
	})

	if len(keysToDelete) > 0 {
		err := dr.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
			for _, k := range keysToDelete {
				if err := tx.Delete(k, nil); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			log.Errorf("Failed to delete keys for table %s: %v", name, err)
		}
	}

	// Clean up field indexes
	for field := range tableVar.Fields {
		if err := dr.RemoveField(id, field); err != nil {
			log.Errorf("Failed to remove field %s for table ID %d: %v", field, id, err)
		}
	}
	tableVar.Fields = nil

	delete(dr.Tables, id)
	delete(dr.nameToId, name)
	delete(dr.idToName, id)
	dr.dropTable(name)
	return nil
}

func (dr *JSONDriver) LookupTableID(name string) (uint16, error) {
	dr.Lock.RLock()
	if id, ok := dr.nameToId[name]; ok {
		dr.Lock.RUnlock()
		return id, nil
	}
	// Case-insensitive fallback for existing tables
	lower := strings.ToLower(name)
	for existing, id := range dr.nameToId {
		if strings.ToLower(existing) == lower {
			dr.Lock.RUnlock()
			return id, nil
		}
	}
	dr.Lock.RUnlock()

	tinfo, err := dr.getTableInfo(name)
	if err != nil {
		return 0, err
	}
	dr.Lock.Lock()
	dr.nameToId[name] = tinfo.TableId
	dr.idToName[tinfo.TableId] = name
	dr.Lock.Unlock()
	return tinfo.TableId, nil
}

func (dr *JSONDriver) ListTableIDs() []uint16 {
	dr.Lock.RLock()
	defer dr.Lock.RUnlock()
	ids := make([]uint16, 0, len(dr.Tables))
	for id := range dr.Tables {
		ids = append(ids, id)
	}
	return ids
}

func (dr *JSONDriver) GetTableInfo(tableID uint16) (*benchtop.TableInfo, error) {
	prefix := []byte{benchtop.TablePrefix}
	var found *benchtop.TableInfo
	err := dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			val, err := it.Value()
			if err != nil {
				continue
			}
			var tinfo benchtop.TableInfo
			if err := sonic.ConfigFastest.Unmarshal(val, &tinfo); err == nil {
				if tinfo.TableId == tableID {
					found = &tinfo
					return nil
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if found == nil {
		return nil, pebble.ErrNotFound
	}
	return found, nil
}

func (dr *JSONDriver) newJSONTable(name string, columns []benchtop.ColumnDef, fileName string, tableID uint16) (*table.JSONTable, error) {
	store, err := dr.ZoneManager.CreateZone(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to init storage: %w", err)
	}

	out := &table.JSONTable{
		Columns:   columns,
		ColumnMap: make(map[string]int),
		TableId:   tableID,
		FileName:  fileName,
		Name:      name,
		Storage:   store,
		LocLookup: func(id string) (*benchtop.RowLoc, error) {
			val, closer, err := dr.Pkv.Get(benchtop.NewPosKey(tableID, []byte(id)))
			if err != nil {
				return nil, err
			}
			defer closer.Close()
			loc := benchtop.DecodeRowLoc(val)
			if loc == nil {
				return nil, fmt.Errorf("invalid row location for id %s", id)
			}
			return loc, nil
		},
		BufferPool: sync.Pool{
			New: func() any {
				return make([]byte, 0, 4096)
			},
		},
		BlockCache: otter.Must(&otter.Options[string, []byte]{
			MaximumSize: 5000,
		}),
	}

	// Define Loader
	out.BlockLoader = func(ctx context.Context, key string) ([]byte, error) {
		// Key format: "TableId:Section:Offset:Size"
		parts := strings.Split(key, ":")
		if len(parts) != 4 {
			return nil, fmt.Errorf("invalid cache key: %s (expected 4 parts)", key)
		}

		// Parts: TableId (0), Section (1), Offset (2), Size (3)
		sec, _ := strconv.Atoi(parts[1])
		off, _ := strconv.Atoi(parts[2])
		sz, _ := strconv.Atoi(parts[3])

		secId := uint16(sec)
		off32 := uint32(off)
		sz32 := uint32(sz)

		var blockData []byte
		var lastErr error

		// Retry loop for coherence gaps
		for i := 0; i < 10; i++ {
			compressed, err := out.Storage.Get(&benchtop.RowLoc{
				Section: secId,
				Offset:  off32,
				Size:    sz32,
			})
			if err != nil {
				lastErr = err
				time.Sleep(10 * time.Millisecond)
				continue
			}

			blockData, err = block.DecompressBlock(compressed)
			if err == nil {
				return blockData, nil
			}
			lastErr = err
			time.Sleep(20 * time.Millisecond)
		}

		return nil, fmt.Errorf("decompress failed for section %d offset %d size %d after retries: %w", sec, off, sz, lastErr)
	}

	for i, col := range columns {
		out.ColumnMap[col.Key] = i
	}
	return out, nil
}
