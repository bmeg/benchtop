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

	out := &BSONTable{columns: columns, handleLock: sync.Mutex{}, columnMap: map[string]int{}}
	tPath := filepath.Join(dr.base, "TABLES", name)
	f, err := os.Create(tPath)
	if err != nil {
		return nil, err
	}
	out.handle = f
	for n, d := range columns {
		out.columnMap[d.Path] = n
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
		if e, ok := entry[c.Path]; ok {
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

	//The first 4 bytes of the BSON block is the size
	b.handle.Seek(int64(offset), io.SeekStart)
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
			out[n.Path] = b.colUnpack(elem[i], n.Type)
		}
	} else {
		for _, colName := range fields {
			if i, ok := b.columnMap[colName]; ok {
				n := b.columns[i]
				elem := columns.Index(uint(i))
				out[n.Path] = b.colUnpack(elem, n.Type)
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

func (b *BSONTable) Scan(filter []FieldFilter, fields ...string) chan map[string]any {
	//TODO

	return nil
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
			dData, err := b.packData(entry.Value)
			if err != nil {
				//log
			}
			bData, err := bson.Marshal(dData)
			if err != nil {
				//log
			}
			writeSize, err := b.handle.Write(bData)
			if err != nil {
				log.Printf("Loading error: %s", err)
			}
			b.addTableEntryInfo(s, entry.Key, uint64(offset), uint64(writeSize))
			offset += int64(writeSize)
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

/*
func getUint64(f *os.File, pos int64) (uint64, error) {
	f.Seek(pos, io.SeekStart)
	buf := make([]byte, 8)
	_, err := f.Read(buf)
	if err != nil {
		return uint64(0), err
	}
	val := binary.LittleEndian.Uint64(buf)
	return val, nil

}

func (b *BsonTable) OffsetToPosition(offset int64) (int64, error) {
	b.indexLock.Lock()
	defer b.indexLock.Unlock()

	info, err := b.indexHandle.Stat()
	if err != nil {
		return -1, err
	}
	size := info.Size()
	count := size / 8

	spanLow := int64(0)
	spanHigh := count
	for {
		cur := (spanLow + spanHigh) >> 1
		val, err := getUint64(b.indexHandle, cur<<3)
		if err != nil {
			return -1, err
		}
		uVal := int64(val & markMask)
		//fmt.Printf("State: %d - %d %d=%d : %d\n", spanLow, spanHigh, cur, uVal, offset)
		if uVal == offset {
			if val&markBit == markBit {
				return cur, fmt.Errorf("deleting deteted record")
			}
			return cur, nil
		}
		if spanLow == spanHigh {
			break
		}
		if spanLow+1 == spanHigh {
			//fmt.Printf("increment\n")
			spanLow = spanHigh
		} else {
			if uVal > offset {
				spanHigh = cur
			} else if uVal < offset {
				spanLow = cur
			}
		}
	}
	return 0, fmt.Errorf("not found")
}
*/

func (b *BSONTable) Delete(name []byte) error {
	posKey := NewPosKey(b.tableId, name)
	b.db.Delete(posKey, nil)
	//TODO: mark the space so it can be reclaimed
	return nil
}

func (b *BSONTable) Compact() error {
	/*
		b.indexLock.Lock()
		defer b.indexLock.Unlock()

		info, err := b.indexHandle.Stat()
		if err != nil {
			return err
		}
		size := info.Size()
		count := size / 8

		sampleSize := int64(10)

		sampleStart := count - sampleSize
		if sampleStart < 0 {
			sampleStart = 0
		}
		moveSet := []int64{}

		b.indexHandle.Seek(sampleStart<<3, io.SeekStart)
		for curSample := sampleStart; curSample < count; curSample++ {
			buf := make([]byte, 8)
			b.indexHandle.Read(buf)
			val := binary.LittleEndian.Uint64(buf)
			if val&markBit == markBit {

			} else {
				moveSet = append(moveSet, int64(val))
			}
		}

		fmt.Printf("readytomove: %#v\n", moveSet)
	*/
	return nil
}
