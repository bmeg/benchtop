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

const batchSize = 1000

type BSONDriver struct {
	base   string
	Lock   sync.RWMutex
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
		Fields: map[string][]string{},
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
		if err := bsonTable.Init(10); err != nil {
			log.Errorf("Failed to init table %s: %v", tableName, err)
			return nil, fmt.Errorf("failed to init table %s: %v", tableName, err)
		}
		log.Infof("Loaded table %s with vector field %s, HNSW index: %v", tableName, bsonTable.VectorField, bsonTable.HnswIndex != nil)
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
		table.Close()
		if table.handle != nil {
			if syncErr := table.handle.Sync(); syncErr != nil {
				log.Errorf("Error syncing table %s: %v", name, syncErr)
			}
			if closeErr := table.handle.Close(); closeErr != nil {
				log.Errorf("Error closing table %s: %v", name, closeErr)
			} else {
				log.Debugf("Closed table %s", name)
			}
			table.handle = nil
		}
	}
	dr.Tables = make(map[string]*BSONTable) // Clear tables to prevent re-closing
	if dr.db != nil {
		if closeErr := dr.db.Close(); closeErr != nil {
			log.Errorf("Error closing pebble db: %v", closeErr)
		}
		dr.db = nil
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
	log.Infof("Opening %s", tinfo.FileName)
	out := &BSONTable{
		columns:   tinfo.Columns,
		columnMap: map[string]int{},
		db:        dr.db,
		tableId:   tinfo.Id,
		handle:    f,
		Path:      tPath,
		FileName:  tinfo.FileName,
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
	metadataChan := make(chan struct {
		table    *BSONTable
		metadata []struct {
			id           string
			offset, size uint64
		}
		err error
	}, 100)

	startTableGoroutine := func(tableName string) {
		ch := make(chan *benchtop.Row, 100)
		tableChannels[tableName] = ch
		wg.Add(1)
		go func() {
			defer wg.Done()
			var metadata []struct {
				id           string
				offset, size uint64
			}
			var localErr *multierror.Error

			dr.Lock.RLock()
			table, exists := dr.Tables[tableName]
			dr.Lock.RUnlock()
			if !exists {
				newTable, err := dr.New(tableName, nil)
				if err != nil {
					localErr = multierror.Append(localErr, fmt.Errorf("failed to create table %s: %v", tableName, err))
					metadataChan <- struct {
						table    *BSONTable
						metadata []struct {
							id           string
							offset, size uint64
						}
						err error
					}{nil, nil, localErr.ErrorOrNil()}
					return
				}
				table = newTable.(*BSONTable)
				dr.Lock.Lock()
				dr.Tables[tableName] = table
				dr.Lock.Unlock()
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
				if err != nil {
					localErr = multierror.Append(localErr, fmt.Errorf("write error for table %s: %v", tableName, err))
					table.handleLock.Unlock()
					continue
				}
				table.handleLock.Unlock()

				// Record metadata for each record in the batch
				for i, id := range ids {
					metadata = append(metadata, struct {
						id           string
						offset, size uint64
					}{id, offsets[i], uint64(len(bDatas[i]))})
				}
			}
			metadataChan <- struct {
				table    *BSONTable
				metadata []struct {
					id           string
					offset, size uint64
				}
				err error
			}{table, metadata, localErr.ErrorOrNil()}

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

	var errs *multierror.Error
	done := make(chan struct{})
	go func() {
		defer close(done)

		writeFunc := func(tx *pebblebulk.PebbleBulk) error {
			for meta := range metadataChan {
				if meta.err != nil {
					errs = multierror.Append(errs, meta.err)
					continue
				}
				if meta.table == nil || len(meta.metadata) == 0 {
					continue
				}
				for _, m := range meta.metadata {
					meta.table.addTableDeleteEntryInfo(tx, []byte(m.id), meta.table.Name)
					meta.table.addTableEntryInfo(tx, []byte(m.id), m.offset, m.size)
				}
			}
			return nil
		}

		var err error
		if tx == nil {
			err = dr.Pb.BulkWrite(writeFunc)
		} else {
			writeFunc(tx)
		}
		if err != nil {
			errs = multierror.Append(errs, err)
		}
	}()

	wg.Wait()
	close(metadataChan)
	<-done

	return errs.ErrorOrNil()
}
