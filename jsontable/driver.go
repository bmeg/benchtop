package jsontable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/cache"
	jTable "github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/benchtop/jsontable/tpath"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/benchtop/util"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"github.com/hashicorp/go-multierror"
)

const (
	BATCH_SIZE = 1000
)

type JSONDriver struct {
	base       string
	Lock       sync.RWMutex
	PebbleLock sync.RWMutex
	Pkv        *pebblebulk.PebbleKV
	LocCache   *cache.JSONCache

	Tables      map[string]*jTable.JSONTable
	LabelLookup map[uint16]string
	Fields      map[string]map[string]any
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
	fmt.Println("TABLE DIR: ", tableDir, exist)
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
		LocCache: cache.NewJSONCache(Pkv),

		Fields:      map[string]map[string]any{},
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
		Fields:      map[string]map[string]any{},
		Lock:        sync.RWMutex{},
		PebbleLock:  sync.RWMutex{},
		LabelLookup: map[uint16]string{},
	}

	err = driver.LoadFields()
	if err != nil {
		pKv.Close()
		return nil, err
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

	if p, ok := dr.Tables[name]; ok {
		return p, nil
	}

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
	dr.Fields = make(map[string]map[string]any)
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

// BulkLoad efficiently loads a large number of rows from an input channel into
// the appropriate tables. It processes rows in batches, distributing them to
// the correct partition and section files based on the new architecture.
//
// tx: A pebblebulk.PebbleBulk instance for transactional batch writing of index
//
//	and row location metadata. If nil, an error will be returned.
func (dr *JSONDriver) BulkLoad(inputs chan *benchtop.Row, tx *pebblebulk.PebbleBulk) error {
	if dr.Pkv == nil || dr.Pkv.Db == nil {
		return fmt.Errorf("pebble database instance is nil")
	}

	var wg sync.WaitGroup
	tableChannels := make(map[string]chan *benchtop.Row)

	// fieldKeyElements holds the components needed to build index keys.
	type fieldKeyElements struct {
		field     string
		tableName string
		val       any
		rowId     string
	}

	// metadataChan is used to pass results from table-specific processing
	// goroutines to a single writer goroutine.
	metadataChan := make(chan struct {
		table                 *jTable.JSONTable
		fieldIndexKeyElements []fieldKeyElements
		metadata              map[string]*benchtop.RowLoc
		err                   error
	}, 100)

	// startTableGoroutine creates and manages a dedicated goroutine for each table.
	startTableGoroutine := func(tableName string) {
		ch := make(chan *benchtop.Row, BATCH_SIZE)
		tableChannels[tableName] = ch
		wg.Add(1)

		go func() {
			snapshot := dr.Pkv.Db.NewSnapshot()
			defer func() {
				snapshot.Close()
				wg.Done()
			}()

			var allFieldIndexKeyElements []fieldKeyElements
			allMetadata := make(map[string]*benchtop.RowLoc)
			var localErr *multierror.Error

			// Get or create the JSONTable instance.
			dr.Lock.RLock()
			table, exists := dr.Tables[tableName]
			dr.Lock.RUnlock()
			if !exists {
				newTable, err := dr.New(tableName, nil)
				if err != nil {
					localErr = multierror.Append(localErr, fmt.Errorf("failed to create table %s: %v", tableName, err))
					metadataChan <- struct {
						table                 *jTable.JSONTable
						fieldIndexKeyElements []fieldKeyElements
						metadata              map[string]*benchtop.RowLoc
						err                   error
					}{nil, nil, nil, localErr.ErrorOrNil()}
					return
				}
				table = newTable.(*jTable.JSONTable)
				dr.Lock.Lock()
				dr.Tables[tableName] = table
				dr.Lock.Unlock()
			}

			// Process rows from the channel in batches.
			for {
				batch := make([]*benchtop.Row, 0, BATCH_SIZE)
				for range BATCH_SIZE {
					row, ok := <-ch
					if !ok {
						break // Channel closed and drained
					}
					batch = append(batch, row)
				}
				if len(batch) == 0 {
					break // Exit loop when channel is done
				}

				// Filter out existing rows and gather index data for new rows.
				newRows := make([]*benchtop.Row, 0, len(batch))
				for _, row := range batch {
					info, err := table.GetTableEntryInfo(snapshot, row.Id) // Assumes this function exists
					if err != nil {
						localErr = multierror.Append(localErr, fmt.Errorf("error getting entry info for %s: %v", row.Id, err))
						continue
					}
					if info == nil { // Row is new
						newRows = append(newRows, row)
						if fields, ok := dr.Fields[tableName]; ok {
							for field := range fields {
								if val := tpath.PathLookup(row.Data, field); val != nil { // Assumes PathLookup exists
									allFieldIndexKeyElements = append(allFieldIndexKeyElements, fieldKeyElements{
										field:     field,
										tableName: tableName,
										val:       val,
										rowId:     string(row.Id),
									})
								}
							}
						}
					}
				}
				if len(newRows) == 0 {
					continue
				}

				// Group new rows by their target partition.
				rowsByPartition := make(map[uint8][]*benchtop.Row)
				for _, row := range newRows {
					partitionId := table.PartitionFunc(row.Id)
					rowsByPartition[partitionId] = append(rowsByPartition[partitionId], row)
				}

				// Process each partition's group of rows.
				for partitionId, rowsInPartition := range rowsByPartition {
					if len(rowsInPartition) == 0 {
						continue
					}

					bDatas := make([][]byte, 0, len(rowsInPartition))
					rowIds := make([]string, 0, len(rowsInPartition))
					var totalDataSize uint32

					// Marshal all data for this partition's batch.
					for _, row := range rowsInPartition {
						bData, err := sonic.ConfigFastest.Marshal(table.PackData(row.Data, string(row.Id)))
						if err != nil {
							localErr = multierror.Append(localErr, fmt.Errorf("marshal error for row %s: %v", row.Id, err))
							continue
						}
						bDatas = append(bDatas, bData)
						rowIds = append(rowIds, string(row.Id))
						totalDataSize += uint32(len(bData)) + benchtop.ROW_HSIZE
					}
					if len(bDatas) == 0 {
						continue
					}

					// Get the current active section for this partition.
					table.SectionLock.Lock()
					secId := table.PartitionMap[partitionId][len(table.PartitionMap[partitionId])-1]
					sec := table.Sections[secId]
					table.SectionLock.Unlock()

					// Lock the section and check if a new one is needed.
					sec.Lock.Lock()
					if sec.LiveBytes+totalDataSize > table.MaxSectionSize {
						sec.Lock.Unlock() // Unlock old section

						newSec, err := table.CreateNewSection(partitionId)
						if err != nil {
							localErr = multierror.Append(localErr, fmt.Errorf("failed to create new section for partition %d", partitionId))
							continue
						}
						sec = newSec
						sec.Lock.Lock() // Lock new section
					}

					startOffset, err := sec.Handle.Seek(0, io.SeekEnd)
					if err != nil {
						localErr = multierror.Append(localErr, fmt.Errorf("seek error for section %d: %v", sec.ID, err))
						sec.Lock.Unlock()
						continue
					}

					// Prepare and write the entire batch payload for this partition.
					batchPayload := make([]byte, totalDataSize)
					currentPos, currentOffset := 0, uint32(startOffset)
					for i, bData := range bDatas {
						nextOffset := currentOffset + benchtop.ROW_HSIZE + uint32(len(bData))
						binary.LittleEndian.PutUint32(batchPayload[currentPos:currentPos+int(benchtop.ROW_OFFSET_HSIZE)], nextOffset)
						binary.LittleEndian.PutUint32(batchPayload[currentPos+int(benchtop.ROW_OFFSET_HSIZE):currentPos+int(benchtop.ROW_HSIZE)], uint32(len(bData)))
						copy(batchPayload[currentPos+int(benchtop.ROW_HSIZE):], bData)

						// Record metadata (including the new Section ID).
						allMetadata[rowIds[i]] = &benchtop.RowLoc{
							TableId: table.TableId,
							Section: sec.ID,
							Offset:  currentOffset,
							Size:    uint32(len(bData)),
						}
						currentOffset = nextOffset
						currentPos += int(benchtop.ROW_HSIZE) + len(bData)
					}

					if _, err := sec.Handle.Write(batchPayload); err != nil {
						localErr = multierror.Append(localErr, fmt.Errorf("write error for section %d: %v", sec.ID, err))
						sec.Lock.Unlock()
						continue
					}

					// Update section statistics.
					sec.TotalRows += uint32(len(bDatas))
					sec.LiveBytes += totalDataSize
					sec.Lock.Unlock()
				}
			}

			// Send all collected metadata for this table to the writer.
			metadataChan <- struct {
				table                 *jTable.JSONTable
				fieldIndexKeyElements []fieldKeyElements
				metadata              map[string]*benchtop.RowLoc
				err                   error
			}{table, allFieldIndexKeyElements, allMetadata, localErr.ErrorOrNil()}
		}()
	}

	// Distribute incoming rows to their respective table goroutines.
	for row := range inputs {
		if _, exists := tableChannels[row.TableName]; !exists {
			startTableGoroutine(row.TableName)
		}
		tableChannels[row.TableName] <- row
	}
	for _, ch := range tableChannels {
		close(ch)
	}

	// This final goroutine collects all metadata and writes it to PebbleDB.
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

				// Write field index entries.
				for _, keyElements := range meta.fieldIndexKeyElements {
					forwardKey := benchtop.FieldKey(keyElements.field, keyElements.tableName, keyElements.val, []byte(keyElements.rowId))
					if err := tx.Set(forwardKey, []byte{}, nil); err != nil {
						errs = multierror.Append(errs, err)
					}
					BVal, err := sonic.ConfigFastest.Marshal(keyElements.val)
					if err != nil {
						errs = multierror.Append(errs, err)
						continue
					}
					if err := tx.Set(benchtop.RFieldKey(keyElements.tableName, keyElements.field, keyElements.rowId), BVal, nil); err != nil {
						errs = multierror.Append(errs, err)
					}
				}

				// Write row location entries.
				for id, m := range meta.metadata {
					dr.LocCache.Set(id, m)
					dr.AddTableEntryInfo(tx, []byte(id), m)
				}
			}
			return nil
		}

		if tx == nil {
			errs = multierror.Append(errs, fmt.Errorf("pebble bulk instance passed into BulkLoad function is nil"))
		} else {
			dr.PebbleLock.Lock()
			if err := writeFunc(tx); err != nil {
				errs = multierror.Append(errs, err)
			}
			dr.PebbleLock.Unlock()
		}
	}()

	wg.Wait()
	close(metadataChan)
	<-done

	return errs.ErrorOrNil()
}
