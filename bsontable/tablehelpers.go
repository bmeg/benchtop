package bsontable

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"go.mongodb.org/mongo-driver/bson"
)

func (b *BSONTable) packData(entry map[string]any, key string) (bson.D, error) {
	// pack named columns
	columns := []any{}
	for _, c := range b.columns {
		if e, ok := entry[c.Name]; ok {
			v, err := benchtop.CheckType(e, c.Type)
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
	return bson.D{{Key: "columns", Value: columns}, {Key: "data", Value: other}, {Key: "key", Value: key}}, nil
}

func (b *BSONTable) addTableEntryInfo(tx *pebblebulk.PebbleBulk, name []byte, offset, size uint64) {
	value := benchtop.NewPosValue(offset, size)
	posKey := benchtop.NewPosKey(b.tableId, name)
	if tx != nil {
		tx.Set(posKey, value, nil)
	} else {
		b.db.Set(posKey, value, nil)
	}

}

func (b *BSONTable) colUnpack(v bson.RawElement, colType benchtop.FieldType) any {
	if colType == benchtop.String {
		return v.Value().StringValue()
	} else if colType == benchtop.Double {
		return v.Value().Double()
	} else if colType == benchtop.Int64 {
		return v.Value().Int64()
	} else if colType == benchtop.Bytes {
		_, data := v.Value().Binary()
		return data
	}
	return nil
}

func (b *BSONTable) getBlockPos(id []byte) (uint64, uint64, error) {
	idKey := benchtop.NewPosKey(b.tableId, id)
	val, closer, err := b.db.Get(idKey)
	if err != nil {
		return 0, 0, err
	}
	offset, size := benchtop.ParsePosValue(val)
	closer.Close()
	return offset, size, nil
}

func (b *BSONTable) setIndices(inputs chan benchtop.Index) {
	for index := range inputs {
		b.addTableEntryInfo(nil, index.Key, index.Position, 0)
	}
}

func (b *BSONTable) markDelete(offset uint64) error {
	file, err := os.OpenFile(b.Path, os.O_RDWR, 0644)
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
	err = file.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (b *BSONTable) readFromFile(offset uint64) (map[string]any, error) {
	file, err := os.Open(b.Path)
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

func (b *BSONTable) writeBsonEntry(offset int64, bData []byte) (int, error) {
	// make next offset equal to existing offset + length of data
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, uint64(offset)+uint64(len(bData))+8)
	b.handle.Write(buffer)
	return b.handle.Write(bData)
}
