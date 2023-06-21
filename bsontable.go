package benchtop

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	//NOTE: try github.com/dgraph-io/ristretto for cache
)

const (
	markBit  = uint64(1) << 63
	markMask = ^(uint64(1) << 63)
)

type BsonTable struct {
	columns     []ColumnDef
	columnMap   map[string]int
	handle      *os.File
	indexHandle *os.File

	handleLock *sync.Mutex
	indexLock  *sync.Mutex
}

func Create(path string, columns []ColumnDef) (TableStore, error) {
	out := BsonTable{columns: columns, handleLock: &sync.Mutex{}, indexLock: &sync.Mutex{}, columnMap: map[string]int{}}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	for n, d := range columns {
		out.columnMap[d.Path] = n
	}

	out.handle = f
	i, err := os.Create(path + ".idx")
	if err != nil {
		f.Close()
		return nil, err
	}
	out.indexHandle = i
	return &out, nil
}

func (b *BsonTable) Close() {
	b.handle.Close()
	b.indexHandle.Close()
}

func (b *BsonTable) GetColumns() []ColumnDef {
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

func (b *BsonTable) Add(entry map[string]any) (int64, error) {
	columns := []any{}
	for _, c := range b.columns {
		if e, ok := entry[c.Path]; ok {
			v, err := checkType(e, c.Type)
			if err != nil {
				return -1, err
			}
			columns = append(columns, v)
		} else {
			columns = append(columns, nil)
		}
	}
	other := map[string]any{}
	for k, v := range entry {
		if _, ok := b.columnMap[k]; !ok {
			other[k] = v
		}
	}
	bData, err := bson.Marshal(bson.D{{Key: "columns", Value: columns}, {Key: "data", Value: other}})
	if err != nil {
		return -1, err
	}
	b.handleLock.Lock()
	defer b.handleLock.Unlock()
	offset, err := b.handle.Seek(0, io.SeekEnd)
	if err != nil {
		return -1, err
	}
	b.handle.Write(bData)
	buf := make([]byte, 8) //binary.MaxVarintLen64)
	b.indexLock.Lock()
	defer b.indexLock.Unlock()
	binary.LittleEndian.PutUint64(buf, uint64(offset))
	b.indexHandle.Seek(0, io.SeekEnd)
	b.indexHandle.Write(buf)
	return offset, nil
}

func (b *BsonTable) colUnpack(v bson.RawElement, colType FieldType) any {
	if colType == String {
		return v.Value().StringValue()
	} else if colType == Double {
		return v.Value().Double()
	} else if colType == Int64 {
		return v.Value().Int64()
	}
	return nil
}

func (b *BsonTable) Get(offset int64, fields ...string) (map[string]any, error) {
	b.handleLock.Lock()
	defer b.handleLock.Unlock()
	b.handle.Seek(offset, io.SeekStart)
	sizeBytes := []byte{0x00, 0x00, 0x00, 0x00}
	_, err := b.handle.Read(sizeBytes)
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

func (b *BsonTable) ListOffsets() (chan int64, error) {
	out := make(chan int64, 10)

	go func() {
		defer close(out)
		b.indexLock.Lock()
		defer b.indexLock.Unlock()
		b.indexHandle.Seek(0, io.SeekStart)
		for {
			buf := make([]byte, 8)
			_, err := b.indexHandle.Read(buf)
			if err == io.EOF {
				break
			}
			var offset = binary.LittleEndian.Uint64(buf)
			v := int64(offset)
			if v >= 0 {
				out <- v
			}
		}
	}()

	return out, nil
}

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

func (b *BsonTable) Delete(offset int64) error {
	//Do something here
	pos, err := b.OffsetToPosition(offset)
	if err != nil {
		return err
	}
	b.indexLock.Lock()
	defer b.indexLock.Unlock()
	b.indexHandle.Seek(pos<<3, io.SeekStart)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(offset)|markBit)
	b.indexHandle.Write(buf)
	return nil
}

func (b *BsonTable) Compact() error {
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

	return nil
}
