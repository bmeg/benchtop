package benchtop

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"go.mongodb.org/mongo-driver/bson"
)

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

func (b *BSONTable) setIndices(inputs chan Index) {
	b.bulkSet(func(s dbSet) error {
		for index := range inputs {
			b.addTableEntryInfo(b.db, index.Key, index.Position, 0)
		}
		return nil
	})
}

func (b *BSONTable) debugCheck(offset uint64) {
	file, err := os.Open(b.path)
	if err != nil {
		fmt.Println("Failed to open file:", err)
		return
	}
	defer file.Close()

	seekOffset := int64(offset + 8)
	_, err = file.Seek(seekOffset, io.SeekStart)
	if err != nil {
		fmt.Println("Failed to seek:", err)
		return
	}

	sizeBytes := make([]byte, 4)
	_, err = file.Read(sizeBytes)
	if err != nil {
		fmt.Println("Failed to read size bytes:", err)
		return
	}

	fmt.Printf("Size bytes at offset %d: %x\n", seekOffset, sizeBytes)
}

func (b *BSONTable) markDelete(offset uint64) error {
	file, err := os.OpenFile(b.path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(int64(offset+8), io.SeekStart)
	if err != nil {
		return err
	}
	_, err = file.Write([]byte{0x00, 0x00, 0x00, 0x00})
	if err != nil {
		return err
	}
	err = file.Sync() // Add this line!
	if err != nil {
		return err
	}

	fmt.Printf("Marked delete at offset %d\n", int64(offset+8))
	return nil
}

func (b *BSONTable) readFromFile(offset uint64) (map[string]any, error) {
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

	file.Seek(-4, io.SeekCurrent)

	rowData := make([]byte, int32(binary.LittleEndian.Uint32(sizeBytes)))
	_, err = file.Read(rowData)
	if err != nil {
		return nil, err
	}

	bd := bson.Raw(rowData)
	columns := bd.Index(0).Value().Array()
	out := map[string]any{}

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

	return out, nil
}

func (b *BSONTable) WriteOffset(buffer []byte, offset int64, bData []byte) (int, error) {
	// make next offset equal to existing offset + length of data
	binary.LittleEndian.PutUint64(buffer, uint64(offset)+uint64(len(bData))+8)
	b.handle.Write(buffer)
	return b.handle.Write(bData)
}
