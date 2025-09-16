package jsontable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/cache"
	jTable "github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/benchtop/util"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
)

const (
	BATCH_SIZE int = 1000
)

type JSONDriver struct {
	base       string
	Lock       sync.RWMutex
	PebbleLock sync.RWMutex
	Pkv        *pebblebulk.PebbleKV
	LocCache   *cache.JSONCache

	Tables      map[string]*jTable.JSONTable
	LabelLookup map[uint16]string
}

func NewJSONDriver(path string) (benchtop.TableDriver, error) {
	Pkv, err := pebblebulk.NewPebbleKV(path)
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
			Pkv.Db.Close()
			return nil, fmt.Errorf("failed to create TABLES directory: %v", err)
		}
	}

	driver := &JSONDriver{
		base:   path,
		Tables: map[string]*jTable.JSONTable{},
		Pkv: &pebblebulk.PebbleKV{
			Db:           Pkv.Db,
			InsertCount:  0,
			CompactLimit: uint32(1000),
		},
		LocCache:    cache.NewJSONCache(Pkv),
		Lock:        sync.RWMutex{},
		PebbleLock:  sync.RWMutex{},
		LabelLookup: map[uint16]string{},
	}

	return driver, nil
}

// Update LoadJSONDriver to use DirExists
func LoadJSONDriver(path string) (benchtop.TableDriver, error) {
	pKv, err := pebblebulk.NewPebbleKV(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	tableDir := filepath.Join(path, "TABLES")
	exist, err := util.DirExists(tableDir)
	if err != nil {
		pKv.Close()
		return nil, err
	}
	if !exist {
		pKv.Close()
		return nil, fmt.Errorf("TABLES directory not found at %s", tableDir)
	}

	driver := &JSONDriver{
		base:   path,
		Tables: map[string]*jTable.JSONTable{},
		Pkv: &pebblebulk.PebbleKV{
			Db:           pKv.Db,
			InsertCount:  0,
			CompactLimit: uint32(1000),
		},
		LocCache:    cache.NewJSONCache(pKv),
		Lock:        sync.RWMutex{},
		PebbleLock:  sync.RWMutex{},
		LabelLookup: map[uint16]string{},
	}

	for _, tableName := range driver.List() {
		table, err := driver.Get(tableName)
		if err != nil {
			driver.Close()
			return nil, fmt.Errorf("failed to load table %s: %v", tableName, err)
		}
		jsonTable, ok := table.(*jTable.JSONTable)
		if !ok {
			driver.Close()
			return nil, fmt.Errorf("invalid table type for %s", tableName)
		}

		driver.Lock.Lock()
		driver.LabelLookup[jsonTable.TableId] = tableName[2:]
		driver.Tables[tableName] = jsonTable
		driver.Lock.Unlock()
	}

	err = driver.LoadFields()
	if err != nil {
		pKv.Close()
		return nil, err
	}

	driver.Lock.RLock()
	err = driver.LocCache.PreloadCache()
	driver.Lock.RUnlock()
	if err != nil {
		return nil, err
	}

	return driver, nil
}

func (dr *JSONDriver) New(name string, columns []benchtop.ColumnDef) (benchtop.TableStore, error) {
	dr.Lock.RLock()
	if p, ok := dr.Tables[name]; ok {
		dr.Lock.RUnlock()
		return p, nil
	}
	dr.Lock.RUnlock()

	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	newId := dr.getMaxTablePrefix()
	formattedName := util.PadToSixDigits(int(newId))
	tPath := filepath.Join(dr.base, "TABLES", formattedName)

	out := &jTable.JSONTable{
		Columns:   columns,
		ColumnMap: map[string]int{},
		Path:      tPath,
		Name:      name,
		FileName:  tPath, // Base name for partition/section files
		TableId:   newId,
		Fields:    map[string]struct{}{},
	}
	for n, d := range columns {
		out.ColumnMap[d.Key] = n
	}

	dr.LabelLookup[newId] = name[2:]

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
		log.Errorf("TABLE INIT ERR: %v", err)
		return nil, fmt.Errorf("failed to init table %s: %v", name, err)
	}

	dr.Tables[name] = out
	log.Debugf("Created table %s", name)
	return out, nil
}

func (dr *JSONDriver) SetIndices(inputs chan benchtop.Index) {
	dr.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		for index := range inputs {
			dr.AddTableEntryInfo(
				tx,
				index.Key,
				&index.Loc,
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

func (dr *JSONDriver) Close() {
	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	log.Infoln("Closing JSONDriver...")
	for tableName, table := range dr.Tables {
		table.Close() // Closes all section handles and file pools
		log.Debugf("Closed table %s", tableName)
	}
	dr.Tables = make(map[string]*jTable.JSONTable)
	if dr.Pkv.Db != nil {
		if closeErr := dr.Pkv.Db.Close(); closeErr != nil {
			log.Errorf("Error closing Pebble database: %v", closeErr)
		}
		dr.Pkv.Db = nil
		time.Sleep(50 * time.Millisecond)
	}
	dr.Pkv = nil
	log.Infof("Successfully closed JSONDriver for path %s", dr.base)
}

func (dr *JSONDriver) Get(name string) (benchtop.TableStore, error) {
	dr.Lock.RLock()
	if x, ok := dr.Tables[name]; ok {
		dr.Lock.RUnlock()
		return x, nil
	}
	dr.Lock.RUnlock()

	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	if x, ok := dr.Tables[name]; ok {
		return x, nil
	}

	nkey := benchtop.NewTableKey([]byte(name))
	value, closer, err := dr.Pkv.Db.Get(nkey)
	if err != nil {
		log.Errorln("JSONDriver Get: ", err)
		return nil, err
	}
	defer closer.Close()
	tinfo := benchtop.TableInfo{}
	if err := sonic.ConfigFastest.Unmarshal(value, &tinfo); err != nil {
		return nil, fmt.Errorf("failed to unmarshal table info: %v", err)
	}

	log.Debugf("Opening Table: %#v\n", tinfo)
	tPath := filepath.Join(dr.base, "TABLES", string(tinfo.FileName))
	out := &jTable.JSONTable{
		Columns:   tinfo.Columns,
		ColumnMap: map[string]int{},
		TableId:   tinfo.TableId,
		Path:      tPath,
		FileName:  tPath,
		Name:      name,
		Fields:    map[string]struct{}{},
	}
	for n, d := range out.Columns {
		out.ColumnMap[d.Key] = n
	}

	if err := out.Init(10); err != nil {
		return nil, fmt.Errorf("failed to init table %s: %v", name, err)
	}
	dr.Tables[name] = out
	return out, nil
}

func (dr *JSONDriver) Delete(name string) error {
	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	table, exists := dr.Tables[name]
	if !exists {
		return fmt.Errorf("table %s does not exist", name)
	}

	table.Close() // Close all section files

	// Delete all section files for the table
	for _, sec := range table.Sections {
		if err := os.Remove(sec.Path); err != nil {
			log.Errorf("Failed to delete section file %s: %v", sec.Path, err)
		}
	}
	delete(dr.Tables, name)
	dr.dropTable(name)
	return nil
}
