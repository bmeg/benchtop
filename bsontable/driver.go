package bsontable

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/benchtop/util"
	"github.com/bmeg/grip/log"
	"github.com/cockroachdb/pebble"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/maypok86/otter/v2"
	"github.com/bytedance/sonic"
)

const BATCH_SIZE = 1000
const ROW_HSIZE = 12
const ROW_OFFSET_HSIZE = 8

type BSONDriver struct {
	base       string
	Lock       sync.RWMutex
	PebbleLock sync.Mutex
	db         *pebble.DB
	Pb         *pebblebulk.PebbleKV

	PageCache   *otter.Cache[string, benchtop.RowLoc]
	PageLoader  otter.LoaderFunc[string, benchtop.RowLoc]

	Tables map[string]*BSONTable
	LabelLookup map[uint16]string
	// Fields is defined like label, field
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

	driver := &BSONDriver{
		base:   path,
		db:     db,
		Tables: map[string]*BSONTable{},
		Pb: &pebblebulk.PebbleKV{
			Db:           db,
			InsertCount:  0,
			CompactLimit: uint32(1000),
		},
		PageCache: otter.Must(&otter.Options[string, benchtop.RowLoc]{
			MaximumSize: 10_000_000,
		}),
		Fields:     map[string]map[string]struct{}{},
		Lock:       sync.RWMutex{},
		PebbleLock: sync.Mutex{},
		LabelLookup: map[uint16]string{},
	}

	driver.PageLoader = otter.LoaderFunc[string, benchtop.RowLoc](func(ctx context.Context, key string) (benchtop.RowLoc, error) {
		log.Debugln("Cache miss, loading from pebble: ", key)
		val, closer, err := driver.Pb.Db.Get([]byte(key))
		if err != nil {
			if err != pebble.ErrNotFound {
				log.Errorf("Err on dr.Pb.Get for key %s in CacheLoader: %v", key, err)
			}
			return benchtop.RowLoc{}, err
		}
		offset, size := benchtop.ParsePosValue(val)
		closer.Close()
		return benchtop.RowLoc{Offset: offset, Size: size}, nil
	})
	return driver, nil
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
		Fields:     map[string]map[string]struct{}{},
		Lock:       sync.RWMutex{},
		PebbleLock: sync.Mutex{},
		PageCache: otter.Must(&otter.Options[string, benchtop.RowLoc]{
			MaximumSize: 10000000,
		}),
		LabelLookup: map[uint16]string{},
	}
	
	err = driver.LoadFields()
	if err != nil {
		return nil, err
	}
	

	for _, tableName := range driver.List() {
		table, err := driver.Get(tableName)
		if err != nil {
			driver.Close()
			return nil, fmt.Errorf("failed to load table %s: %v", tableName, err)
		}
		bsonTable, ok := table.(*BSONTable)
		if !ok {
			driver.Close()
			log.Errorf("invalid table type for %s", tableName)
			return nil, fmt.Errorf("invalid table type for %s", tableName)
		}
		// Pb is already set in Get, but ensure consistency if needed
		bsonTable.Pb = &pebblebulk.PebbleKV{
			Db:           db,
			InsertCount:  0,
			CompactLimit: uint32(1000),
		}
		driver.Lock.Lock()
		driver.LabelLookup[bsonTable.TableId] = tableName[2:]
		driver.Tables[tableName] = bsonTable
		driver.Lock.Unlock()
	}

	driver.PageLoader = otter.LoaderFunc[string, benchtop.RowLoc](func(ctx context.Context, key string) (benchtop.RowLoc, error) {
		log.Debugln("Cache miss, loading from pebble: ", key)
		val, closer, err := driver.Pb.Db.Get([]byte(key))
		if err != nil {
			if err != pebble.ErrNotFound {
				log.Errorf("Err on dr.Pb.Get for key %s in CacheLoader: %v", key, err)
			}
			return benchtop.RowLoc{}, err
		}
		offset, size := benchtop.ParsePosValue(val)
		defer closer.Close()
		return benchtop.RowLoc{Offset: offset, Size: size}, nil
	})

	driver.Lock.RLock()
	err = driver.PreloadCache()
	driver.Lock.RUnlock()
	if err != nil {
		return nil, err
	}

	return driver, nil
}

func (dr *BSONDriver) New(name string, columns []benchtop.ColumnDef) (benchtop.TableStore, error) {
	dr.Lock.RLock()
	if p, ok := dr.Tables[name]; ok {
		dr.Lock.RUnlock()
		return p, nil
	}
	dr.Lock.RUnlock()

	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	if p, ok := dr.Tables[name]; ok {
		return p, nil
	}

	newId := dr.getMaxTablePrefix()
	formattedName := util.PadToSixDigits(int(newId))
	tPath := filepath.Join(dr.base, "TABLES", formattedName)
	f, err := os.Create(tPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create table %s: %v", tPath, err)
	}

	out := &BSONTable{
		columns:    columns,
		handleLock: sync.RWMutex{},
		columnMap:  map[string]int{},
		Path:       tPath,
		Name:       name,
		FileName:   formattedName,
		handle:     f,
		db:         dr.db,
		Pb: &pebblebulk.PebbleKV{
			Db:           dr.db,
			InsertCount:  0,
			CompactLimit: uint32(1000),
		},
		TableId: newId,
	}
	for n, d := range columns {
		out.columnMap[d.Key] = n
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
		f.Close()
		return nil, fmt.Errorf("failed to marshal table info: %v", err)
	}
	
	if err := dr.addTable(tinfo.Name, outData); err != nil {
		f.Close()
		log.Errorf("Error adding table: %s", err)
		return nil, err
	}

	buffer := make([]byte, 12)
	binary.LittleEndian.PutUint64(buffer[:8], uint64(0) + uint64(len(outData))+12)
	binary.LittleEndian.PutUint32(buffer[8:12], uint32(len(outData)))

	if _, err := out.handle.Write(buffer); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to write table header: %v", err)
	}
	if _, err := out.handle.Write(outData); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to write table data: %v", err)
	}

	if err := out.Init(10); err != nil {
		f.Close()
		log.Errorln("TABLE POOL ERR: %v", err)
		return nil, fmt.Errorf("failed to init table %s: %v", name, err)
	}

	dr.Tables[name] = out
	log.Debugf("Created table %s with FilePool: %v", name, out.FilePool)
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
	for tableName, table := range dr.Tables {
		table.handleLock.Lock()
		if table.handle != nil {
			if syncErr := table.handle.Sync(); syncErr != nil {
				log.Errorf("Error syncing table %s handle: %v", tableName, syncErr)
			}
			if closeErr := table.handle.Close(); closeErr != nil {
				log.Errorf("Error closing table %s handle: %v", tableName, closeErr)
			} else {
				log.Debugf("Closed table %s", tableName)
			}
			table.handle = nil
		}
		table.handleLock.Unlock()
		table.Pb = nil
	}
	dr.Tables = make(map[string]*BSONTable)
	if dr.db != nil {
		if closeErr := dr.db.Close(); closeErr != nil {
			log.Errorf("Error closing Pebble database: %v", closeErr)
		}
		dr.db = nil
		time.Sleep(50 * time.Millisecond)
	}
	dr.Pb = nil
	dr.Fields = make(map[string]map[string]struct{})
	log.Infof("Successfully closed BSONDriver for path %s", dr.base)
	return
}

func (dr *BSONDriver) Get(name string) (benchtop.TableStore, error) {
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
	value, closer, err := dr.db.Get(nkey)
	if err != nil {
		log.Errorln("BSONDriver Get: ", err)
		return nil, err
	}
	defer closer.Close()
	tinfo := benchtop.TableInfo{}
	sonic.ConfigFastest.Unmarshal(value, &tinfo)

	log.Debugf("Opening Table: %#v\n", tinfo)
	tPath := filepath.Join(dr.base, "TABLES", string(tinfo.FileName))
	f, err := os.OpenFile(tPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open table %s: %v", tPath, err)
	}

	out := &BSONTable{
		columns:    tinfo.Columns,
		db:         dr.db,
		columnMap:  map[string]int{},
		TableId:    tinfo.TableId,
		handle:     f,
		handleLock: sync.RWMutex{},
		Path:       tPath,
		FileName:   tinfo.FileName,
		Name:       name,
		Pb: &pebblebulk.PebbleKV{
			Db:           dr.db,
			InsertCount:  0,
			CompactLimit: uint32(1000),
		},
	}
	for n, d := range out.columns {
		out.columnMap[d.Key] = n
	}

	if out.FilePool == nil {
		if err := out.Init(10); err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to init table %s: %v", name, err)
		}
	}
	dr.Tables[name] = out
	return out, nil
}

// Currently not used
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

// BulkLoad
// tx: set null to initialize pebble bulk write context
func (dr *BSONDriver) BulkLoad(inputs chan *benchtop.Row, tx *pebblebulk.PebbleBulk) error {
	
	if dr.Pb == nil || dr.Pb.Db == nil {
        return fmt.Errorf("pebble database instance is nil")
    }
	var wg sync.WaitGroup
	tableChannels := make(map[string]chan *benchtop.Row)
	metadataChan := make(chan struct {
		table          *BSONTable
		fieldIndexKeys [][]byte
		metadata       map[string]benchtop.RowLoc
		err            error
	}, 100)


	startTableGoroutine := func(tableName string) {
		snapshot := dr.Pb.Db.NewSnapshot()

		ch := make(chan *benchtop.Row, 100)
		tableChannels[tableName] = ch
		wg.Add(1)
		go func() {
   			defer func() {
      			snapshot.Close()
                wg.Done()
            }()
   			var fieldIndexKeys [][]byte
			metadata := make(map[string]benchtop.RowLoc)
			var localErr *multierror.Error

			dr.Lock.RLock()
			table, exists := dr.Tables[tableName]
			dr.Lock.RUnlock()
			if !exists {
				newTable, err := dr.New(tableName, nil)
				if err != nil {
					localErr = multierror.Append(localErr, fmt.Errorf("failed to create table %s: %v", tableName, err))
					metadataChan <- struct {
						table          *BSONTable
						fieldIndexKeys [][]byte
						metadata       map[string]benchtop.RowLoc
						err            error
					}{nil, nil, nil, localErr.ErrorOrNil()}
					return
				}
				table = newTable.(*BSONTable)
				dr.Lock.Lock()
				dr.Tables[tableName] = table
				dr.Lock.Unlock()
			}
			for {
				batch := make([]*benchtop.Row, 0, BATCH_SIZE)
				for range BATCH_SIZE {
					row, ok := <-ch
					if !ok {
						break
					}
					batch = append(batch, row)
				}
				if len(batch) == 0 {
					break
				}

				bDatas := make([][]byte, 0, BATCH_SIZE)
				ids := make([]string, 0, BATCH_SIZE)
				for _, row := range batch {
					_, fieldsExist := dr.Fields[tableName]
					if fieldsExist {
						for field := range dr.Fields[tableName] {
							if val := PathLookup(row.Data, field); val != nil {
								fieldIndexKeys = append(fieldIndexKeys, benchtop.FieldKey(field, tableName, val, row.Id))
							}
						}
					}

					bData, err := sonic.ConfigFastest.Marshal(
						table.packData(row.Data, string(row.Id)),
					)
					if err != nil {
						localErr = multierror.Append(localErr, fmt.Errorf("marshal data error for table %s: %v", tableName, err))
						continue
					}

					info, err := table.getTableEntryInfo(snapshot, row.Id)
					if err != nil {
						localErr = multierror.Append(localErr, fmt.Errorf("error getting entry info for %s: %v", row.Id, err))
						continue
					}

					if info == nil {
						bDatas = append(bDatas, bData)
						ids = append(ids, string(row.Id))
					}
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
				totalLen := 0
				for i, bData := range bDatas {
					offsets[i+1] = offsets[i] + ROW_HSIZE + uint64(len(bData))
					totalLen += ROW_HSIZE + len(bData)
				}

				batchData := make([]byte, totalLen)
				pos := 0
				for i, bData := range bDatas {
					binary.LittleEndian.PutUint64(batchData[pos:pos + ROW_OFFSET_HSIZE], offsets[i+1])
					binary.LittleEndian.PutUint32(batchData[pos + ROW_OFFSET_HSIZE: pos + ROW_HSIZE], uint32(len(bData)))
					pos += ROW_HSIZE + len(bData)
					copy(batchData[pos - len(bData):pos], bData)
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
					metadata[id] = benchtop.RowLoc{Offset: offsets[i], Size: uint64(len(bDatas[i])), Label: table.TableId}
				}
			}

			metadataChan <- struct {
				table          *BSONTable
				fieldIndexKeys [][]byte
				metadata       map[string]benchtop.RowLoc
				err            error
			}{table, fieldIndexKeys, metadata, localErr.ErrorOrNil()}
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
				if meta.table == nil {
					continue
				}
				for _, key := range meta.fieldIndexKeys {
					err := tx.Set(key, []byte{}, nil)
					if err != nil {
						errs = multierror.Append(errs, err)
					}
				}

				for id, m := range meta.metadata {
					dr.PageCache.Set(id, m)
					meta.table.AddTableEntryInfo(tx, []byte(id), m)
				}
			}
			return nil
		}

		var err error
		if tx == nil {
			errs = multierror.Append(errs, fmt.Errorf("pebble bulk instance passed into BulkLoad function is nil"))
		} else {
			dr.PebbleLock.Lock()
			err = writeFunc(tx)
			dr.PebbleLock.Unlock()
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
