package bsontable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/benchtop/util"
	"github.com/bmeg/grip/log"
	"github.com/cockroachdb/pebble"
	multierror "github.com/hashicorp/go-multierror"
	"go.mongodb.org/mongo-driver/bson"
)

type BSONDriver struct {
	base   string
	lock   sync.RWMutex
	db     *pebble.DB
	Pb     *pebblebulk.PebbleKV
	Tables map[string]*BSONTable
	Fields map[string][]string
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
	}, nil
}

func (dr *BSONDriver) New(name string, columns []benchtop.ColumnDef) (benchtop.TableStore, error) {
	p, _ := dr.Get(name)
	if p != nil {
		return p, fmt.Errorf("table %s already exists", name)
	}

	dr.lock.Lock()
	defer dr.lock.Unlock()

	tPath := filepath.Join(dr.base, "TABLES", name)
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
		out.columnMap[d.Name] = n
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
	it, _ := dr.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
		value := benchtop.ParseTableKey(it.Key())
		out = append(out, string(value))
	}
	it.Close()
	return out
}

func (dr *BSONDriver) Close() {
	log.Infoln("Closing driver")
	for _, i := range dr.Tables {
		i.handle.Close()
	}
	dr.db.Close()
}

func (dr *BSONDriver) Get(name string) (benchtop.TableStore, error) {
	dr.lock.Lock()
	defer dr.lock.Unlock()

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

	tPath := filepath.Join(dr.base, "TABLES", name)

	f, err := os.Open(tPath)
	if err != nil {
		return nil, err
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
	dr.lock.Lock()
	defer dr.lock.Unlock()

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

	tPath := filepath.Join(dr.base, "TABLES", name)
	if err := os.Remove(tPath); err != nil {
		return fmt.Errorf("failed to delete table file %s: %v", tPath, err)
	}
	delete(dr.Tables, name)

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
					label := string(row.Label)

					dr.lock.RLock()
					table, exists := dr.Tables[label]
					dr.lock.RUnlock()
					if !exists {
						mu.Lock()
						errs = multierror.Append(errs, fmt.Errorf("table %s not found after creation", label))
						mu.Unlock()
						return
					}

					dData, err := table.packData(row.Data, string(row.Id))
					if err != nil {
						mu.Lock()
						log.Errorf("pack data error for table %s: %v", label, err)
						errs = multierror.Append(errs, fmt.Errorf("pack data error for table %s: %v", label, err))
						mu.Unlock()
						return
					}
					bData, err := bson.Marshal(dData)
					if err != nil {
						mu.Lock()
						log.Errorf("bson marshal error for table %s: %v", label, err)
						errs = multierror.Append(errs, fmt.Errorf("bson marshal error for table %s: %v", label, err))
						mu.Unlock()
						return
					}

					table.handleLock.Lock()
					defer table.handleLock.Unlock()
					offset, err := table.handle.Seek(0, io.SeekEnd)
					if err != nil {
						mu.Lock()
						errs = multierror.Append(errs, fmt.Errorf("seek error for table %s: %v", label, err))
						mu.Unlock()
						return
					}

					writeSize, err := table.writeBsonEntry(offset, bData)
					if err != nil {
						log.Errorf("write error for table %s: %v", label, err)
						mu.Lock()
						errs = multierror.Append(errs, fmt.Errorf("write error for table %s: %v", label, err))
						mu.Unlock()
						return
					}

					//log.Infof("ID: %s, OFFSET: %d, WRITE SIZE: %d", row.Id, offset, writeSize)
					table.addTableEntryInfo(tx, row.Id, uint64(offset), uint64(writeSize))
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
		label := string(row.Label)
		dr.lock.RLock()
		_, exists := dr.Tables[label]
		dr.lock.RUnlock()
		if !exists {
			log.Infof("Creating new table for: %s", label)
			newTable, err := dr.New(label, nil)
			if err != nil {
				mu.Lock()
				errs = multierror.Append(errs, fmt.Errorf("failed to create table %s: %v", label, err))
				mu.Unlock()
			} else {
				dr.lock.Lock()
				dr.Tables[label] = newTable.(*BSONTable)
				dr.lock.Unlock()
			}
		}
		rowChan <- row
	}

	close(rowChan)
	wg.Wait()
	log.Infoln("Ending bulk load")

	return errs.ErrorOrNil()
}
