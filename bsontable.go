package benchtop

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"

	"go.mongodb.org/mongo-driver/bson"
	//NOTE: try github.com/dgraph-io/ristretto for cache
)

type BSONDriver struct {
	base string
	db   *pebble.DB

	lock   sync.Mutex
	tables map[string]*BSONTable
}

type BSONTable struct {
	columns    []ColumnDef
	columnMap  map[string]int
	handle     *os.File
	db         *pebble.DB
	tableId    uint32
	handleLock sync.Mutex
}

type dbSet interface {
	Set(id []byte, val []byte, opts *pebble.WriteOptions) error
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
	log.Println("Closing driver")
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
	out := &BSONTable{columns: columns,
		handleLock: sync.Mutex{}, columnMap: map[string]int{}}
	tPath := filepath.Join(dr.base, "TABLES", name)
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
		log.Printf("Error: %s", err)
	}
	if err := dr.addTableEntry(newID, name, columns); err != nil {
		log.Printf("Error: %s", err)
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
	// make Next offset equal to existing offset + length of data
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
	b.handle.Read(rowData) //read the full block
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

func (b *BSONTable) Keys() (chan []byte, error) {
	out := make(chan []byte, 10)
	go func() {
		defer close(out)

		prefix := NewPosKeyPrefix(b.tableId)
		it, err := b.db.NewIter(&pebble.IterOptions{})
		if err != nil {
			log.Printf("error: %s", err)
		}
		for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			_, value := ParsePosKey(it.Key())
			out <- value
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
				//log
			}
			bData, err := bson.Marshal(dData)
			if err != nil {
				//log
			}

			bsonHandlerNextoffset := make([]byte, 8)
			// make Next offset equal to existing offset + length of data
			binary.LittleEndian.PutUint64(bsonHandlerNextoffset, uint64(offset)+uint64(len(bData))+8)
			b.handle.Write(bsonHandlerNextoffset)

			writeSize, err := b.handle.Write(bData)
			if err != nil {
				log.Printf("Loading error: %s", err)
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
	defer tempHandle.Close()

	oldHandle := b.handle
	_, err = oldHandle.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	defer oldHandle.Close()

	newOffsets := make(map[string]uint64)
	var newOffset uint64 = 0

	offsetSizeData := make([]byte, 8)
	sizeBytes := make([]byte, 4)

	for {
		_, err := oldHandle.Read(offsetSizeData)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		NextOffset := binary.LittleEndian.Uint64(offsetSizeData)
		_, err = oldHandle.Read(sizeBytes)
		if err != nil {
			return err
		}

		bSize := int32(binary.LittleEndian.Uint32(sizeBytes))
		if bSize == 0 {
			_, err = oldHandle.Seek(int64(NextOffset), io.SeekStart)
			if err == io.EOF {
				break
			}
			continue
		}

		rowData := make([]byte, bSize)
		copy(rowData, sizeBytes)

		_, err = oldHandle.Read(rowData[4:])
		if err != nil {
			return err
		}

		raw := bson.Raw(rowData)

		arr, _ := raw.Lookup("columns").Array().Values()
		keyName := arr[0].StringValue()
		newOffsets[keyName] = newOffset

		newOffsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(newOffsetBytes, newOffset+uint64(len(rowData))+8)
		_, err = tempHandle.Write(newOffsetBytes)
		if err != nil {
			return err
		}

		_, err = tempHandle.Write(rowData)
		if err != nil {
			return err
		}

		newOffset += uint64(len(rowData)) + 8
	}

	oldHandle.Close()
	fileName, err := filepath.Abs(b.handle.Name())
	if err != nil {
		return err
	}
	err = os.Rename(tempFileName, fileName)
	if err != nil {
		return err
	}

	b.handle, err = os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	for key, newPos := range newOffsets {
		b.addTableEntryInfo(b.db, []byte(key), newPos, 0) // 0 size assumes the same size as before
	}

	return nil
}
