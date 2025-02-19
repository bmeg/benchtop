package benchtop

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bmeg/grip/log"

	"github.com/cockroachdb/pebble"

	"go.mongodb.org/mongo-driver/bson"
	//NOTE: try github.com/dgraph-io/ristretto for cache
)

type BSONDriver struct {
	base string
	db   *pebble.DB

	lock   sync.RWMutex
	tables map[string]*BSONTable
}

type BSONTable struct {
	columns    []ColumnDef
	columnMap  map[string]int
	handle     *os.File
	db         *pebble.DB
	tableId    uint32
	handleLock sync.RWMutex
	path       string
}

type dbSet interface {
	Set(id []byte, val []byte, opts *pebble.WriteOptions) error
}

type dbGet interface {
	Get(key []byte) ([]byte, io.Closer, error)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return err != os.ErrNotExist
}

func NewBSONDriver(path string) (TableDriver, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	tableDir := filepath.Join(path, "TABLES")
	if fileExists(tableDir) {
		os.Mkdir(tableDir, 0700)
	}
	return &BSONDriver{base: path, db: db, tables: map[string]*BSONTable{}}, nil
}

func (dr *BSONDriver) Close() {
	log.Infoln("Closing driver")
	for _, i := range dr.tables {
		i.handle.Close()
	}
	dr.db.Close()
}

func (dr *BSONDriver) Get(name string) (TableStore, error) {
	dr.lock.Lock()
	defer dr.lock.Unlock()

	if x, ok := dr.tables[name]; ok {
		return x, nil
	}

	nkey := NewNameKey([]byte(name))
	value, closer, err := dr.db.Get(nkey)
	if err != nil {
		return nil, err
	}
	tinfo := TableInfo{}
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
		path:    tPath,
	}
	dr.tables[name] = out

	return out, nil
}

func (dr *BSONDriver) getMaxTableID() uint32 {
	// get unique id
	prefix := []byte{idPrefix}
	it, _ := dr.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	maxID := uint32(0)
	for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
		value := ParseIDKey(it.Key())
		maxID = value
	}
	it.Close()
	return maxID
}

func (dr *BSONDriver) addTableEntry(id uint32, name string, columns []ColumnDef) error {
	tdata, _ := bson.Marshal(TableInfo{Columns: columns, Id: id})
	nkey := NewNameKey([]byte(name))
	return dr.db.Set(nkey, tdata, nil)
}

func (dr *BSONDriver) addTableID(newID uint32, name string) error {
	idKey := NewIDKey(newID)
	return dr.db.Set(idKey, []byte(name), nil)
}

func (dr *BSONDriver) New(name string, columns []ColumnDef) (TableStore, error) {

	p, _ := dr.Get(name)
	if p != nil {
		return p, fmt.Errorf("table %s already exists", name)
	}

	dr.lock.Lock()
	defer dr.lock.Unlock()

	// Prepend Key column to columns provided by user
	columns = append([]ColumnDef{{Name: "keyName", Type: String}}, columns...)

	tPath := filepath.Join(dr.base, "TABLES", name)
	out := &BSONTable{columns: columns,
		handleLock: sync.RWMutex{}, columnMap: map[string]int{},
		path: tPath}
	f, err := os.Create(tPath)
	if err != nil {
		return nil, err
	}
	out.handle = f
	for n, d := range columns {
		out.columnMap[d.Name] = n
	}

	newID := dr.getMaxTableID() + 1

	if err := dr.addTableID(newID, name); err != nil {
		log.Errorf("Error: %s", err)
	}
	if err := dr.addTableEntry(newID, name, columns); err != nil {
		log.Errorf("Error: %s", err)
	}
	out.db = dr.db
	out.tableId = newID
	dr.tables[name] = out
	return out, nil
}

func (dr *BSONDriver) List() []string {
	out := []string{}
	prefix := []byte{namePrefix}
	it, _ := dr.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
		value := ParseNameKey(it.Key())
		out = append(out, string(value))
	}
	it.Close()
	return out
}

func (b *BSONTable) Close() {
	//because the table could be opened by other threads, don't actually close
	//b.handle.Close()
	//b.db.Close()
}

func (b *BSONTable) GetColumns() []ColumnDef {
	return b.columns
}

func checkType(val any, t FieldType) (any, error) {
	switch t {
	case Int64:
		if x, ok := val.(int32); !ok {
			return int64(x), nil
		}
		if _, ok := val.(int64); !ok {
			return val, fmt.Errorf("not int64")
		}
	}
	return val, nil
}

func (b *BSONTable) packData(entry map[string]any) (bson.D, error) {
	// pack named columns
	columns := []any{}
	for _, c := range b.columns {
		if e, ok := entry[c.Name]; ok {
			v, err := checkType(e, c.Type)
			if err != nil {
				return nil, err
			}
			columns = append(columns, v)
		} else {
			columns = append(columns, nil)
		}
	}
	// pack all other data
	other := map[string]any{}
	for k, v := range entry {
		if _, ok := b.columnMap[k]; !ok {
			other[k] = v
		}
	}
	return bson.D{{Key: "columns", Value: columns}, {Key: "data", Value: other}}, nil
}

func (b *BSONTable) addTableEntryInfo(db dbSet, name []byte, offset, size uint64) {
	value := NewPosValue(offset, size)
	posKey := NewPosKey(b.tableId, name)
	db.Set(posKey, value, nil)
}

func (b *BSONTable) Add(id []byte, entry map[string]any) error {
	entry["keyName"] = string(id)
	dData, err := b.packData(entry)
	if err != nil {
		return err
	}

	bData, err := bson.Marshal(dData)
	if err != nil {
		return err
	}
	//append to end of block file
	b.handleLock.Lock()
	defer b.handleLock.Unlock()
	offset, err := b.handle.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	bsonHandlerNextoffset := make([]byte, 8)
	// make next offset equal to existing offset + length of data
	binary.LittleEndian.PutUint64(bsonHandlerNextoffset, uint64(offset)+uint64(len(bData))+8)
	b.handle.Write(bsonHandlerNextoffset)

	b.handle.Write(bData)
	b.addTableEntryInfo(b.db, id, uint64(offset), uint64(len(bData)))

	return nil
}

func (b *BSONTable) Get(id []byte, fields ...string) (map[string]any, error) {
	b.handleLock.Lock()
	defer b.handleLock.Unlock()

	offset, _, err := b.getBlockPos(id)
	if err != nil {
		return nil, err
	}

	// Offset skip the first 8 bytes since they are for getting the offset for a scan operation
	b.handle.Seek(int64(offset+8), io.SeekStart)
	//The next 4 bytes of the BSON block is the size
	sizeBytes := []byte{0x00, 0x00, 0x00, 0x00}
	_, err = b.handle.Read(sizeBytes)
	if err != nil {
		return nil, err
	}
	bSize := int32(binary.LittleEndian.Uint32(sizeBytes))
	b.handle.Seek(-4, io.SeekCurrent)
	rowData := make([]byte, bSize)
	b.handle.Read(rowData)
	bd := bson.Raw(rowData)
	columns := bd.Index(0).Value().Array()
	out := map[string]any{}
	if len(fields) == 0 {
		if err := bd.Index(1).Value().Unmarshal(&out); err != nil {
			return nil, err
		}
		elem, err := columns.Elements()
		if err != nil {
			return nil, err
		}
		for i, n := range b.columns {
			out[n.Name] = b.colUnpack(elem[i], n.Type)
		}
	} else {
		for _, colName := range fields {
			if i, ok := b.columnMap[colName]; ok {
				n := b.columns[i]
				elem := columns.Index(uint(i))
				out[n.Name] = b.colUnpack(elem, n.Type)
			}
		}
	}
	return out, nil
}

func (b *BSONTable) colUnpack(v bson.RawElement, colType FieldType) any {
	if colType == String {
		return v.Value().StringValue()
	} else if colType == Double {
		return v.Value().Double()
	} else if colType == Int64 {
		return v.Value().Int64()
	} else if colType == Bytes {
		_, data := v.Value().Binary()
		return data
	}
	return nil
}

func (b *BSONTable) getBlockPos(id []byte) (uint64, uint64, error) {
	idKey := NewPosKey(b.tableId, id)
	val, closer, err := b.db.Get(idKey)
	if err != nil {
		return 0, 0, err
	}
	offset, size := ParsePosValue(val)
	closer.Close()
	return offset, size, nil
}

func (b *BSONTable) Keys() (chan Index, error) {
	out := make(chan Index, 10)
	go func() {
		defer close(out)

		prefix := NewPosKeyPrefix(b.tableId)
		it, err := b.db.NewIter(&pebble.IterOptions{})
		if err != nil {
			log.Errorf("error: %s", err)
		}
		for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			_, value := ParsePosKey(it.Key())
			out <- Index{Key: value}
		}
		it.Close()
	}()
	return out, nil
}

func (b *BSONTable) Scan(filter []FieldFilter, fields ...string) (chan map[string]any, error) {
	b.handleLock.Lock()
	defer b.handleLock.Unlock()

	out := make(chan map[string]any, 10)
	_, err := b.handle.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(out)
		for {
			offsetSizeData := make([]byte, 8)
			_, err := b.handle.Read(offsetSizeData)
			if err == io.EOF {
				break
			}
			if err != nil {
				return
			}

			NextOffset := binary.LittleEndian.Uint64(offsetSizeData)

			sizeBytes := make([]byte, 4)
			_, err = b.handle.Read(sizeBytes)
			if err != nil {
				return
			}

			bSize := int32(binary.LittleEndian.Uint32(sizeBytes))

			// Elem has been deleted. skip it.
			if bSize == 0 {
				_, err = b.handle.Seek(int64(NextOffset), io.SeekStart)
				if err == io.EOF {
					break
				}
				continue
			}
			rowData := make([]byte, bSize)
			copy(rowData, sizeBytes)

			_, err = b.handle.Read(rowData[4:])
			if err != nil {
				return
			}

			bd := bson.Raw(rowData)
			columns := bd.Index(0).Value().Array()

			vOut := map[string]any{}
			for _, colName := range fields {
				if i, ok := b.columnMap[colName]; ok {
					n := b.columns[i]
					unpack := b.colUnpack(columns.Index(uint(i)), n.Type)
					if PassesFilters(unpack, filter) {
						vOut[n.Name] = unpack
					}
				}
			}
			if len(vOut) > 0 {
				out <- vOut
			}

			_, err = b.handle.Seek(int64(NextOffset), io.SeekStart)
			if err == io.EOF {
				break
			}
		}
	}()
	return out, nil
}

func (b *BSONTable) Fetch(inputs chan Index, workers int) <-chan struct {
	key  string
	data map[string]any
	err  string
} {
	log.Infoln("INPUtS", inputs)

	results := make(chan struct {
		key  string
		data map[string]any
		err  string
	}, workers)

	var wg sync.WaitGroup
	offsetChan := make(chan struct {
		key    string
		offset uint64
	}, workers)

	// Step 1: Fetch offsets from Pebble DB
	go func() {
		b.bulkGet(func(s dbGet) error {
			for entry := range inputs {

				wg.Add(1)
				go func(index Index) {
					defer wg.Done()

					// Get offset from Pebble batch
					val, closer, err := s.Get(NewPosKey(b.tableId, index.Key))

					if err != nil {
						if err == pebble.ErrNotFound {
							results <- struct {
								key  string
								data map[string]any
								err  string
							}{string(index.Key), nil, func() string {
								if err != nil {
									return err.Error()
								}
								return ""
							}()}
							return
						}
						results <- struct {
							key  string
							data map[string]any
							err  string
						}{string(index.Key), nil, func() string {
							if err != nil {
								return err.Error()
							}
							return ""
						}()}
						return
					}
					defer closer.Close()

					// Convert stored value to offset
					offset, _ := ParsePosValue(val)

					offsetChan <- struct {
						key    string
						offset uint64
					}{string(index.Key), offset}
				}(entry)
			}
			return nil
		})

		// Close offset channel when all goroutines finish
		wg.Wait()
		close(offsetChan)
	}()

	// Step 2: Use worker pool for parallel file reads with RWLock
	go func() {
		var fileWg sync.WaitGroup
		sem := make(chan struct{}, workers)
		for entry := range offsetChan {
			fileWg.Add(1)
			sem <- struct{}{}

			go func(e struct {
				key    string
				offset uint64
			}) {
				defer fileWg.Done()
				defer func() { <-sem }()

				data, err := b.readFromFileWithLock(e.offset)
				if err != nil {
					data = nil
				}
				results <- struct {
					key  string
					data map[string]any
					err  string
				}{e.key, data, func() string {
					if err != nil {
						return err.Error()
					}
					return ""
				}()}
			}(entry)
		}

		// Close results channel after all file reads complete
		fileWg.Wait()
		close(results)
	}()

	return results
}

func (b *BSONTable) Load(inputs chan Entry) error {

	b.handleLock.Lock()
	defer b.handleLock.Unlock()
	offset, err := b.handle.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	b.bulkWrite(func(s dbSet) error {
		for entry := range inputs {
			entry.Value["keyName"] = string(entry.Key)
			dData, err := b.packData(entry.Value)
			if err != nil {
				log.Errorf("pack data err in Load: bulkWrite: %s", err)
			}
			bData, err := bson.Marshal(dData)
			if err != nil {
				log.Errorf("bson Marshall err in Load: bulkWrite: %s", err)
			}

			bsonHandlerNextoffset := make([]byte, 8)
			// make Next offset equal to existing offset + length of data
			binary.LittleEndian.PutUint64(bsonHandlerNextoffset, uint64(offset)+uint64(len(bData))+8)
			b.handle.Write(bsonHandlerNextoffset)

			writeSize, err := b.handle.Write(bData)
			if err != nil {
				log.Errorf("write handler err in Load: bulkWrite: %s", err)
			}
			b.addTableEntryInfo(s, entry.Key, uint64(offset), uint64(writeSize))
			offset += int64(writeSize) + 8
		}
		return nil
	})

	return nil
}

type pebbleBulkWrite struct {
	db              *pebble.DB
	batch           *pebble.Batch
	highest, lowest []byte
	curSize         int
}

type pebbleBulkRead struct {
	db *pebble.DB
}

const (
	maxWriterBuffer = 3 << 30
)

func copyBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func (pbw *pebbleBulkWrite) Set(id []byte, val []byte, opts *pebble.WriteOptions) error {
	pbw.curSize += len(id) + len(val)
	if pbw.highest == nil || bytes.Compare(id, pbw.highest) > 0 {
		pbw.highest = copyBytes(id)
	}
	if pbw.lowest == nil || bytes.Compare(id, pbw.lowest) < 0 {
		pbw.lowest = copyBytes(id)
	}
	err := pbw.batch.Set(id, val, opts)
	if pbw.curSize > maxWriterBuffer {
		pbw.batch.Commit(nil)
		pbw.batch.Reset()
		pbw.curSize = 0
	}
	return err
}

func (pbr *pebbleBulkRead) Get(key []byte) ([]byte, io.Closer, error) {
	return pbr.db.Get(key)
}

func (b *BSONTable) bulkGet(u func(s dbGet) error) error {
	return u(&pebbleBulkRead{b.db})
}

func (b *BSONTable) bulkWrite(u func(s dbSet) error) error {
	batch := b.db.NewBatch()
	ptx := &pebbleBulkWrite{b.db, batch, nil, nil, 0}
	err := u(ptx)
	batch.Commit(nil)
	batch.Close()
	if ptx.lowest != nil && ptx.highest != nil {
		b.db.Compact(ptx.lowest, ptx.highest, true)
	}
	return err
}

func (b *BSONTable) Delete(name []byte) error {
	b.handleLock.Lock()
	defer b.handleLock.Unlock()

	offset, _, err := b.getBlockPos(name)
	if err != nil {
		return err
	}
	b.handle.Seek(int64(offset+8), io.SeekStart)
	_, err = b.handle.Write([]byte{0x00, 0x00, 0x00, 0x00})
	if err != nil {
		return err
	}

	posKey := NewPosKey(b.tableId, name)
	b.db.Delete(posKey, nil)

	return nil
}

func (b *BSONTable) Compact() error {
	const flushThreshold = 1000
	flushCounter := 0
	b.handleLock.Lock()
	defer b.handleLock.Unlock()

	tempFileName, err := filepath.Abs(b.handle.Name() + ".compact")
	if err != nil {
		return err
	}

	tempHandle, err := os.Create(tempFileName)
	if err != nil {
		return err
	}

	oldHandle := b.handle
	_, err = oldHandle.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	reader := bufio.NewReaderSize(oldHandle, 16*1024*1024)
	writer := bufio.NewWriterSize(tempHandle, 16*1024*1024)
	defer writer.Flush()

	//newOffsets := make(map[string]uint64)
	var newOffset uint64 = 0

	offsetSizeData := make([]byte, 8)
	sizeBytes := make([]byte, 4)

	rowBuff := make([]byte, 0, 1<<20)

	fileOffset := int64(0)
	inputChan := make(chan Index, 100)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.SetIndices(inputChan)
	}()

	for {
		startLoop := time.Now()

		// --- Read next offset ---
		startReadOffset := time.Now()
		_, err := io.ReadFull(reader, offsetSizeData)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed reading next offset: %w", err)
		}
		nextOffset := binary.LittleEndian.Uint64(offsetSizeData)
		log.Debugf("Time to read offset: %v\n", time.Since(startReadOffset))

		// --- Read BSON object size (4 bytes) ---
		startReadSize := time.Now()
		_, err = io.ReadFull(reader, sizeBytes)
		if err != nil {
			return fmt.Errorf("failed reading size: %w", err)
		}
		bSize := int32(binary.LittleEndian.Uint32(sizeBytes))
		log.Debugf("Time to read BSON size: %v\n", time.Since(startReadSize))

		// --- Handle empty records ---
		fileOffset += 8 + 4
		if bSize == 0 {
			// Check if there's a gap
			if int64(nextOffset) > fileOffset {
				startSeek := time.Now()
				_, err = oldHandle.Seek(int64(nextOffset), io.SeekStart)
				if err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("failed to seek to nextOffset: %w", err)
				}
				fileOffset = int64(nextOffset) // Update fileOffset
				reader.Reset(oldHandle)
				log.Debugf("Time to seek & reset reader: %v\n", time.Since(startSeek))
			}
			continue
		}

		startReadBSON := time.Now()
		if int(bSize) > cap(rowBuff) {
			rowBuff = make([]byte, bSize)
		} else {
			rowBuff = rowBuff[:bSize]
		}
		copy(rowBuff, sizeBytes)
		_, err = io.ReadFull(reader, rowBuff[4:])
		if err != nil {
			return fmt.Errorf("failed reading BSON data: %w", err)
		}
		log.Debugf("Time to read BSON record: %v\n", time.Since(startReadBSON))

		// --- Extract 'columns' field from BSON ---
		startParseBSON := time.Now()
		raw := bson.Raw(rowBuff)
		val, exists := raw.LookupErr("columns")
		if exists != nil {
			return fmt.Errorf("'columns' field not found in BSON document")
		}
		if val.Type != bson.TypeArray {
			return fmt.Errorf("unexpected BSON type for 'columns': %v", val.Type)
		}
		arr, ok := val.ArrayOK()
		if !ok || len(arr) == 0 {
			return fmt.Errorf("'columns' array is missing or empty")
		}
		keyName, err := arr.IndexErr(0)

		inputChan <- Index{Key: []byte(keyName.Value().StringValue()), Position: newOffset}
		log.Debugf("Time to parse BSON 'columns': %v\n", time.Since(startParseBSON))

		// --- Write new offset ---
		startWrite := time.Now()
		newOffsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(newOffsetBytes, newOffset+uint64(len(rowBuff))+8)

		_, err = writer.Write(newOffsetBytes)
		if err != nil {
			return fmt.Errorf("failed writing new offset: %w", err)
		}
		_, err = writer.Write(rowBuff)
		if err != nil {
			return fmt.Errorf("failed writing BSON row: %w", err)
		}

		flushCounter++
		if flushCounter%flushThreshold == 0 {
			writer.Flush()
		}

		log.Debugf("Time to write new offset and BSON: %v\n", time.Since(startWrite))

		newOffset += uint64(len(rowBuff)) + 8

		log.Debugf("Total loop iteration time: %v\n", time.Since(startLoop))
	}
	close(inputChan)
	wg.Wait()

	// Replace the old file with the compacted one
	fileName, err := filepath.Abs(b.handle.Name())
	if err != nil {
		return err
	}

	tempHandle.Sync()

	err = os.Rename(tempFileName, fileName)
	if err != nil {
		return fmt.Errorf("failed renaming compacted file: %w", err)
	}

	b.handle, err = os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed reopening compacted file: %w", err)
	}

	return nil
}

func (b *BSONTable) SetIndices(inputs chan Index) {
	b.bulkWrite(func(s dbSet) error {
		for index := range inputs {
			b.addTableEntryInfo(b.db, index.Key, index.Position, 0)
		}
		return nil
	})
}

func (b *BSONTable) readFromFileWithLock(offset uint64) (map[string]any, error) {
	file, err := os.Open(b.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = file.Seek(int64(offset+8), io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Read BSON block size
	sizeBytes := []byte{0x00, 0x00, 0x00, 0x00}
	_, err = file.Read(sizeBytes)
	if err != nil {
		return nil, err
	}

	file.Seek(-4, io.SeekCurrent) // Move back to start of block

	// Read BSON block
	rowData := make([]byte, int32(binary.LittleEndian.Uint32(sizeBytes)))
	_, err = file.Read(rowData)
	if err != nil {
		return nil, err
	}

	// Deserialize BSON
	bd := bson.Raw(rowData)
	columns := bd.Index(0).Value().Array()
	out := map[string]any{}

	if err := bd.Index(1).Value().Unmarshal(&out); err != nil {
		return nil, err
	}

	// Extract column values
	elem, err := columns.Elements()
	if err != nil {
		return nil, err
	}
	for i, n := range b.columns {
		out[n.Name] = b.colUnpack(elem[i], n.Type)
	}

	return out, nil
}
