package bsontable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/benchtop/util"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/jsonpath"
	"github.com/cockroachdb/pebble"
	multierror "github.com/hashicorp/go-multierror"
	"go.mongodb.org/mongo-driver/bson"
)

const batchSize = 1000

type BSONDriver struct {
	base   string
	Lock   sync.RWMutex
	db     *pebble.DB
	Pb     *pebblebulk.PebbleKV
	Tables map[string]*BSONTable
	// first map is table name, second map is field jsonpath
	Fields map[string]map[string]struct{}
}

func NewBSONDriver(path string) (benchtop.TableDriver, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	tableDir := filepath.Join(path, "TABLES")
	if util.FileExists(tableDir) {
		os.Mkdir(tableDir, 0700)
	}
	return &BSONDriver{
		base:   path,
		db:     db,
		Tables: map[string]*BSONTable{},
		Pb: &pebblebulk.PebbleKV{
			Db:           db,
			InsertCount:  0,
			CompactLimit: uint32(1000),
		},
		Fields: map[string]map[string]struct{}{},
	}, nil
}

func LoadBSONDriver(path string) (benchtop.TableDriver, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	tableDir := filepath.Join(path, "TABLES")
	if !util.FileExists(tableDir) {
		return nil, fmt.Errorf("TABLES directory not found at %s", tableDir)
	}

	driver := &BSONDriver{
		base:   path,
		db:     db,
		Tables: map[string]*BSONTable{},
		Pb: &pebblebulk.PebbleKV{
			Db:           db,
			InsertCount:  0,
			CompactLimit: uint32(1000),
		},
		Fields: map[string]map[string]struct{}{},
	}

	prefixLen := len(benchtop.FieldPrefix)
	// Keep all of the initialized schema indices in RAM to be used as 'bookmarks' for fetching field values
	err = driver.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(benchtop.FieldPrefix); it.Valid() && bytes.HasPrefix(it.Key(), benchtop.FieldPrefix); it.Next() {
			suffix := it.Key()[prefixLen:]
			// Schema indices have exactly 2 parts: tableName:field
			if bytes.Count(suffix, []byte(":")) != 1 {
				continue
			}
			colonIdx := bytes.Index(suffix, []byte(":"))
			tableName := string(bytes.TrimSpace(suffix[:colonIdx]))
			fieldName := string(bytes.TrimSpace(suffix[colonIdx+1:]))
			if _, exists := driver.Fields[tableName]; !exists {
				driver.Fields[tableName] = map[string]struct{}{}
			}
			driver.Fields[tableName][fieldName] = struct{}{}
		}
		return nil
	})
	if err != nil {
		fmt.Println("ERR: ")
		return nil, err
	}

	tableNames := driver.List()
	for _, tableName := range tableNames {
		table, err := driver.Get(tableName)
		if err != nil {
			driver.Close()
			return nil, fmt.Errorf("failed to load table %s: %v", tableName, err)
		}

		bsonTable, ok := table.(*BSONTable)
		if !ok {
			driver.Close()
			return nil, fmt.Errorf("invalid table type for %s", tableName)
		}

		bsonTable.Pb = &pebblebulk.PebbleKV{
			Db:           db,
			InsertCount:  0,
			CompactLimit: uint32(1000),
		}

		if err := bsonTable.Init(10); err != nil {
			log.Errorf("Failed to init table %s: %v", tableName, err)
			return nil, fmt.Errorf("failed to init table %s: %v", tableName, err)
		}
		driver.Tables[tableName] = bsonTable

	}

	return driver, nil
}

func (dr *BSONDriver) New(name string, columns []benchtop.ColumnDef) (benchtop.TableStore, error) {
	p, _ := dr.Get(name)
	if p != nil {
		return p, fmt.Errorf("table %s already exists", name)
	}

	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	formattedName := util.PadToSixDigits(len(dr.Tables))

	tPath := filepath.Join(dr.base, "TABLES", formattedName)
	out := &BSONTable{
		columns:    columns,
		handleLock: sync.RWMutex{},
		columnMap:  map[string]int{},
		Path:       tPath,
		Name:       name,
		FileName:   formattedName,
	}
	f, err := os.Create(tPath)
	if err != nil {
		return nil, err
	}
	out.handle = f
	for n, d := range columns {
		out.columnMap[d.Key] = n
	}

	outData, err := bson.Marshal(out)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, uint64(0)+uint64(len(outData))+8)
	out.handle.Write(buffer)
	out.handle.Write(outData)

	newId := dr.getMaxTablePrefix()
	if err := dr.addTable(newId, name, columns, formattedName); err != nil {
		log.Errorf("Error: %s", err)
	}

	out.db = dr.db
	out.Pb = &pebblebulk.PebbleKV{
		Db:           dr.db,
		InsertCount:  0,
		CompactLimit: uint32(1000),
	}
	out.tableId = newId
	dr.Tables[name] = out
	if err := out.Init(10); err != nil { // Pool size 10 as example
		log.Errorln("TABLE POOL ERR: ", err)
	}

	return out, nil
}

func (dr *BSONDriver) List() []string {
	out := []string{}
	prefix := []byte{benchtop.TablePrefix}
	dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			value := benchtop.ParseTableKey(it.Key())
			out = append(out, string(value))
		}
		return nil
	})
	return out
}

func (dr *BSONDriver) Close() {
	dr.Lock.Lock()
	defer dr.Lock.Unlock()
	log.Infoln("Closing BSONDriver...")
	for name, table := range dr.Tables {
		if table.handle != nil {
			if syncErr := table.handle.Sync(); syncErr != nil {
				log.Errorf("Error syncing table %s: %v", name, syncErr)
			}
			if closeErr := table.handle.Close(); closeErr != nil {
				log.Errorf("Error closing table %s: %v", name, closeErr)
			} else {
				log.Debugf("Closed table %s", name)
			}
			table.handle = nil // Prevent reuse
		}
	}
	if closeErr := dr.db.Close(); closeErr != nil {
		log.Errorf("Error closing pebble db: %v", closeErr)
	}
}

func (dr *BSONDriver) Get(name string) (benchtop.TableStore, error) {
	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	if x, ok := dr.Tables[name]; ok {
		return x, nil
	}

	nkey := benchtop.NewTableKey([]byte(name))

	value, closer, err := dr.db.Get(nkey)
	if err != nil {
		return nil, err
	}
	tinfo := benchtop.TableInfo{}
	bson.Unmarshal(value, &tinfo)
	closer.Close()

	tPath := filepath.Join(dr.base, "TABLES", string(tinfo.FileName))

	f, err := os.OpenFile(tPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open table %s: %v", tPath, err)
	}
	log.Debugf("Opening %s", tinfo.FileName)
	out := &BSONTable{
		columns:   tinfo.Columns,
		db:        dr.db,
		columnMap: map[string]int{},
		tableId:   tinfo.Id,
		handle:    f,
		Path:      tPath,
		FileName:  tinfo.FileName,
		Name:      name,
	}
	for n, d := range out.columns {
		out.columnMap[d.Key] = n
	}

	dr.Tables[name] = out
	return out, nil
}

func (dr *BSONDriver) Delete(name string) error {
	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	table, exists := dr.Tables[name]
	if !exists {
		return fmt.Errorf("table %s does not exist", name)
	}

	table.handleLock.Lock()
	defer table.handleLock.Unlock()

	if table.handle != nil {
		if err := table.handle.Close(); err != nil {
			log.Errorf("Error closing table %s handle: %v", name, err)
		}
		table.handle = nil
	}

	tPath := filepath.Join(dr.base, "TABLES", string(table.FileName))
	if err := os.Remove(tPath); err != nil {
		return fmt.Errorf("failed to delete table file %s: %v", tPath, err)
	}
	delete(dr.Tables, name)
	dr.dropTable(name)

	return nil
}

func (dr *BSONDriver) DeleteAnyRow(name []byte) error {
	rtasockey := benchtop.NewRowTableAsocKey(name)
	dr.Lock.Lock()
	rtasocval, closer, err := dr.db.Get(rtasockey)
	dr.Lock.Unlock()
	if err != nil {
		return err
	}
	dr.Lock.Lock()
	err = dr.Tables[string(rtasocval)].DeleteRow(name)
	dr.Lock.Unlock()

	if err != nil {
		return err
	}
	closer.Close()
	return nil
}

// BulkLoad
// tx: set null to initialize pebble bulk write context
func (dr *BSONDriver) BulkLoad(inputs chan *benchtop.Row, tx *pebblebulk.PebbleBulk) error {
	var wg sync.WaitGroup
	tableChannels := make(map[string]chan *benchtop.Row)
	errChan := make(chan error, 100)

	startTableGoroutine := func(tableName string) {
		ch := make(chan *benchtop.Row, 100)
		tableChannels[tableName] = ch

		wg.Add(1) // Ensure this is before the goroutine starts
		go func() {
			defer wg.Done()
			var localErr *multierror.Error

			dr.Lock.RLock()
			table, exists := dr.Tables[tableName]
			fieldMap, fieldExists := dr.Fields[tableName[2:]]
			dr.Lock.RUnlock()

			if !exists {
				newTable, err := dr.New(tableName, nil)
				if err != nil {
					errChan <- fmt.Errorf("failed to create table %s: %v", tableName, err)
					return
				}
				table = newTable.(*BSONTable)
				dr.Lock.Lock()
				dr.Tables[tableName] = table
				dr.Lock.Unlock()
			}
			if !fieldExists {
				log.Infof("tableName '%s' does not exist in fieldMap", table.Name[2:])
				return
			}

			compiledPaths := make(map[string]*jsonpath.Compiled)
			for field := range fieldMap {
				compiled, err := jsonpath.Compile("$." + field)
				if err != nil {
					log.Errorf("Failed to compile JSONPath %s: %v", field, err)
					continue
				}
				compiledPaths[field] = compiled
			}

			for {
				batch := make([]*benchtop.Row, 0, batchSize)
				for range batchSize {
					row, ok := <-ch
					if !ok {
						break
					}
					batch = append(batch, row)
				}
				if len(batch) == 0 {
					break
				}

				bDatas := make([][]byte, 0, batchSize)
				ids := make([]string, 0, batchSize)
				rowDatas := make([]map[string]any, 0, batchSize)

				for _, row := range batch {
					mData, err := table.packData(row.Data, string(row.Id))
					if err != nil {
						localErr = multierror.Append(localErr, fmt.Errorf("pack data error for table %s: %v", tableName, err))
						continue
					}
					bData, err := bson.Marshal(mData)
					if err != nil {
						localErr = multierror.Append(localErr, fmt.Errorf("marshal data error for table %s: %v", tableName, err))
						continue
					}
					bDatas = append(bDatas, bData)
					ids = append(ids, string(row.Id))
					rowDatas = append(rowDatas, row.Data)
				}
				if len(bDatas) == 0 {
					continue
				}

				table.handleLock.Lock()
				startOffset, err := table.handle.Seek(0, io.SeekEnd)
				if err != nil {
					localErr = multierror.Append(localErr, fmt.Errorf("seek error for table %s: %v", tableName, err))
					table.handleLock.Unlock()
					continue
				}

				offsets := make([]uint64, len(bDatas)+1)
				offsets[0] = uint64(startOffset)
				for i, bData := range bDatas {
					offsets[i+1] = offsets[i] + 8 + uint64(len(bData))
				}

				var batchData []byte
				for i, bData := range bDatas {
					header := make([]byte, 8)
					binary.LittleEndian.PutUint64(header, offsets[i+1])
					batchData = append(batchData, header...)
					batchData = append(batchData, bData...)
				}

				_, err = table.handle.Write(batchData)
				table.handleLock.Unlock()
				if err != nil {
					localErr = multierror.Append(localErr, fmt.Errorf("write error for table %s: %v", tableName, err))
					continue
				}

				// Use separate wait group for indexing
				var indexWG sync.WaitGroup
				errChanLocal := make(chan error, len(ids)*len(compiledPaths))

				for i, id := range ids {
					indexWG.Add(1)
					go func(i int, id string) {
						defer indexWG.Done()
						table.addTableDeleteEntryInfo(tx, []byte(id), table.Name)
						table.addTableEntryInfo(tx, []byte(id), offsets[i], uint64(len(bDatas[i])))
						for field, compiled := range compiledPaths {
							value, err := compiled.Lookup(rowDatas[i])
							if err != nil {
								continue
							}
							values := flattenValues(value)
							for _, val := range values {
								if err := tx.Set(
									[]byte(string(benchtop.FieldPrefix)+table.Name+":"+field+":"+val+":"+id), nil, nil); err != nil {
									errChanLocal <- fmt.Errorf("failed to set index %s for %s: %v", field, id, err)
								}
							}
						}
					}(i, id)
				}

				// Wait for all indexing goroutines
				indexWG.Wait()
				close(errChanLocal)

				for err := range errChanLocal {
					localErr = multierror.Append(localErr, err)
				}
			}
			if localErr != nil {
				errChan <- localErr.ErrorOrNil()
			}
		}()
	}

	for row := range inputs {
		tableName := row.TableName
		if _, exists := tableChannels[tableName]; !exists {
			startTableGoroutine(tableName)
		}
		tableChannels[tableName] <- row
	}

	for _, ch := range tableChannels {
		close(ch)
	}

	wg.Wait()
	close(errChan)

	var errs *multierror.Error
	for err := range errChan {
		errs = multierror.Append(errs, err)
	}

	return errs.ErrorOrNil()
}

func (dr *BSONDriver) Join(table1, field1, table2, field2 string) (chan struct {
	Row1, Row2 map[string]interface{}
}, error) {
	path1 := fmt.Sprintf("%s.%s", table1, field1)
	path2 := fmt.Sprintf("%s.%s", table2, field2)

	dr.Lock.RLock()
	_, exists1 := dr.Fields[path1]
	_, exists2 := dr.Fields[path2]
	dr.Lock.RUnlock()
	if !exists1 || !exists2 {
		return nil, fmt.Errorf("fields %s or %s are not indexed", path1, path2)
	}

	out := make(chan struct {
		Row1, Row2 map[string]interface{}
	}, 10)
	prefix1 := []byte(fmt.Sprintf("%s:%s:%s", benchtop.FieldPrefix, table1, field1))

	go func() {
		defer close(out)

		// Build a map of values to row IDs for table1
		valueToIDs1 := make(map[string][]string)
		err := dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix1); it.Valid() && bytes.HasPrefix(it.Key(), prefix1); it.Next() {
				parts := strings.Split(string(it.Key()), ":")
				if len(parts) != 4 {
					continue
				}
				value := parts[3]
				ids := string(it.Value())
				valueToIDs1[value] = append(valueToIDs1[value], ids)
			}
			return nil
		})
		if err != nil {
			log.Errorf("Error scanning table1 indices: %v", err)
			return
		}

		// Match against table2
		prefix2 := []byte(fmt.Sprintf("%s:%s:%s", benchtop.FieldPrefix, table2, field2))
		err = dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix2); it.Valid() && bytes.HasPrefix(it.Key(), prefix2); it.Next() {
				parts := strings.Split(string(it.Key()), ":")
				if len(parts) != 4 {
					continue
				}
				value := parts[3]
				id2 := string(it.Value())
				if ids1, ok := valueToIDs1[value]; ok {
					t1, err1 := dr.Get(table1)
					t2, err2 := dr.Get(table2)
					if err1 != nil || err2 != nil {
						continue
					}
					bt1 := t1.(*BSONTable)
					bt2 := t2.(*BSONTable)
					for _, id1 := range ids1 {
						row1, err1 := bt1.GetRow([]byte(id1))
						row2, err2 := bt2.GetRow([]byte(id2))
						if err1 == nil && err2 == nil {
							out <- struct {
								Row1, Row2 map[string]interface{}
							}{Row1: row1, Row2: row2}
						}
					}
				}
			}
			return nil
		})
		if err != nil {
			log.Errorf("Error scanning table2 indices: %v", err)
		}
	}()

	return out, nil
}
