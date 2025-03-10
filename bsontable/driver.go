package bsontable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/akrylysov/pogreb"
	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/benchtop/util"
	"github.com/bmeg/grip/log"
	"github.com/cockroachdb/pebble"
	multierror "github.com/hashicorp/go-multierror"
	"go.mongodb.org/mongo-driver/bson"
)

type BSONDriver struct {
	base           string
	Lock           sync.RWMutex
	pogrebInstance *pogreb.DB
	db             *pebble.DB
	Pb             *pebblebulk.PebbleKV
	Tables         map[string]*BSONTable
	Fields         map[string][]string
}

func NewBSONDriver(path string, pogrebInstance *pogreb.DB) (benchtop.TableDriver, error) {
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
		Fields:         map[string][]string{},
		pogrebInstance: pogrebInstance,
	}, nil
}

func LoadBSONDriver(path string, pogrebInstance *pogreb.DB) (benchtop.TableDriver, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	tableDir := filepath.Join(path, "TABLES")
	if !util.FileExists(tableDir) {
		return nil, fmt.Errorf("TABLES directory not found at %s", tableDir)
	}

	driver := &BSONDriver{
		base:           path,
		db:             db,
		Tables:         map[string]*BSONTable{},
		pogrebInstance: pogrebInstance,
		Pb: &pebblebulk.PebbleKV{
			Db:           db,
			InsertCount:  0,
			CompactLimit: uint32(1000),
		},
		Fields: map[string][]string{},
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

		driver.Tables[tableName] = bsonTable
	}

	return driver, nil
}

func padToSixDigits(number int) string {
	numStr := strconv.Itoa(number)
	numZeros := 6 - len(numStr)
	if numZeros < 0 {
		return numStr
	}
	return strings.Repeat("0", numZeros) + numStr
}

func (dr *BSONDriver) New(name string, columns []benchtop.ColumnDef) (benchtop.TableStore, error) {
	p, _ := dr.Get(name)
	if p != nil {
		return p, fmt.Errorf("table %s already exists", name)
	}

	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	formattedName := padToSixDigits(len(dr.Tables))
	err := dr.pogrebInstance.Put([]byte(name), []byte(formattedName))

	tPath := filepath.Join(dr.base, "TABLES", formattedName)
	out := &BSONTable{
		columns:    columns,
		handleLock: sync.RWMutex{},
		columnMap:  map[string]int{},
		Path:       tPath,
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
	if err := dr.addTable(newId, name, columns); err != nil {
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
		log.Errorf("Error closing db: %v", closeErr)
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

	tName, err := dr.pogrebInstance.Get([]byte(name))
	if err != nil {
		return nil, err
	}
	tPath := filepath.Join(dr.base, "TABLES", string(tName))

	f, err := os.OpenFile(tPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open table %s: %v", tPath, err)
	}

	out := &BSONTable{
		columns: tinfo.Columns,
		db:      dr.db,
		tableId: tinfo.Id,
		handle:  f,
		Path:    tPath,
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
	}

	pgName, err := dr.pogrebInstance.Get([]byte(name))
	if err != nil {
		return err
	}
	tPath := filepath.Join(dr.base, "TABLES", string(pgName))
	if err := os.Remove(tPath); err != nil {
		return fmt.Errorf("failed to delete table file %s: %v", tPath, err)
	}
	delete(dr.Tables, name)

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

func (dr *BSONDriver) BulkLoad(inputs chan *benchtop.Row) error {
	var mu sync.Mutex
	var wg sync.WaitGroup
	var errs *multierror.Error
	rowChan := make(chan *benchtop.Row, 100)

	// Start a goroutine to consume from rowChan
	go func() {
		err := dr.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
			for row := range rowChan {
				wg.Add(1)
				go func(row *benchtop.Row) {
					defer wg.Done()

					dr.Lock.RLock()
					table, exists := dr.Tables[row.TableName]
					dr.Lock.RUnlock()
					if !exists {
						mu.Lock()
						errs = multierror.Append(errs, fmt.Errorf("table %s not found after creation", row.TableName))
						mu.Unlock()
						return
					}

					dData, err := table.packData(row.Data, string(row.Id))
					if err != nil {
						mu.Lock()
						log.Errorf("pack data error for table %s: %v", row.TableName, err)
						errs = multierror.Append(errs, fmt.Errorf("pack data error for table %s: %v", row.TableName, err))
						mu.Unlock()
						return
					}
					bData, err := bson.Marshal(dData)
					if err != nil {
						mu.Lock()
						log.Errorf("bson marshal error for table %s: %v", row.TableName, err)
						errs = multierror.Append(errs, fmt.Errorf("bson marshal error for table %s: %v", row.TableName, err))
						mu.Unlock()
						return
					}

					table.handleLock.Lock()
					defer table.handleLock.Unlock()
					offset, err := table.handle.Seek(0, io.SeekEnd)
					if err != nil {
						mu.Lock()
						errs = multierror.Append(errs, fmt.Errorf("seek error for table %s: %v", row.TableName, err))
						mu.Unlock()
						return
					}

					writeSize, err := table.writeBsonEntry(offset, bData)
					if err != nil {
						log.Errorf("write error for table %s: %v", row.TableName, err)
						mu.Lock()
						errs = multierror.Append(errs, fmt.Errorf("write error for table %s: %v", row.TableName, err))
						mu.Unlock()
						return
					}

					//log.Infof("ID: %s, OFFSET: %d, WRITE SIZE: %d", row.Id, offset, writeSize)
					table.addTableEntryInfo(tx, row.Id, row.TableName, uint64(offset), uint64(writeSize))
					//log.Infof("Finished processing table: %s", label)
				}(row)
			}
			wg.Wait()
			return errs.ErrorOrNil()
		})
		if err != nil {
			mu.Lock()
			errs = multierror.Append(errs, err)
			mu.Unlock()
		}
	}()

	for row := range inputs {
		dr.Lock.RLock()
		_, exists := dr.Tables[row.TableName]
		dr.Lock.RUnlock()
		if !exists {
			log.Debugf("Creating new table for: %s on graph %s", row.TableName, dr.base)

			newTable, err := dr.New(row.TableName, nil)
			if err != nil {
				mu.Lock()
				errs = multierror.Append(errs, fmt.Errorf("failed to create table %s: %v", row.TableName, err))
				mu.Unlock()
			} else {
				dr.Lock.Lock()
				dr.Tables[row.TableName] = newTable.(*BSONTable)
				dr.Lock.Unlock()
			}
		}
		rowChan <- row
	}

	close(rowChan)
	wg.Wait()

	return errs.ErrorOrNil()
}
