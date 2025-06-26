package bsontable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"




	/*"sync"
	"sort"*/

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable/tpath"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/jsonpath"
	"github.com/cockroachdb/pebble"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (b *BSONTable) packData(entry map[string]any, key string) (bson.M, error) {
	// pack named columns
	columns := []any{}
	for _, c := range b.columns {
		if e, ok := entry[c.Key]; ok {
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
	return bson.M{"R": bson.A{columns, other, key}}, nil
}

func (b *BSONTable) addTableDeleteEntryInfo(tx *pebblebulk.PebbleBulk, rowId []byte, label string) {
	rtAsocKey := benchtop.NewRowTableAsocKey(rowId)
	if tx != nil {
		tx.Set(rtAsocKey, []byte(label), nil)
	} else {
		b.db.Set(rtAsocKey, []byte(label), nil)
	}
}
func (b *BSONTable) addTableEntryInfo(tx *pebblebulk.PebbleBulk, rowId []byte, offset, size uint64) {
	value := benchtop.NewPosValue(offset, size)
	posKey := benchtop.NewPosKey(b.tableId, rowId)
	if tx != nil {
		tx.Set(posKey, value, nil)
	} else {
		b.db.Set(posKey, value, nil)
	}
}

type EntryInfo struct {
	Offset uint64
	Size   uint64
}

func PathLookup(v map[string]any, path string) any {
	/* Expects that special fields like '_id' and '_label'
	   are added to the map before reaching this function
	*/
	field := tpath.NormalizePath(path)
	jpath := tpath.ToLocalPath(field)
	res, err := jsonpath.JsonPathLookup(v, jpath)
	if err != nil {
		return nil
	}
	return res
}

func (b *BSONTable) getTableEntryInfo(snap *pebble.Snapshot, id []byte) (*EntryInfo, error) {
	// Really only want to see if anything was returned or not
	_, closer, err := snap.Get(benchtop.NewPosKey(b.tableId, id))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return &EntryInfo{}, nil
}

func (b *BSONTable) unpackData(justKeys bool, doc bson.M) (any, error) {
	row, ok :=  doc["R"].(primitive.A)
	if !ok || len(row) != 3 {
		return nil , errors.New("invalid row format: must be an array of 3 elements")
	}
	if justKeys{
		key, ok := row[2].(string)
		if !ok {
			return nil, errors.New("invalid bson record: expecting string key at index 2")
		}
		return key, nil
	}

	columnsArray, ok := row[0].(primitive.A)
	if !ok || len(columnsArray) != len(b.columns) {
		return nil, errors.New("invalid columns array: must match number of defined columns")
	}

	otherMap, ok := row[1].(bson.M)
	if !ok {
		return nil, errors.New("invalid other map: must be a map")
	}

	result := make(map[string]any, len(b.columns)+len(otherMap))
	for i, col := range b.columns {
		result[col.Key] = convertBSONValue(columnsArray[i])
	}

	for k, v := range otherMap {
		result[k] = convertBSONValue(v)
	}

	result["_id"] = convertBSONValue(row[2])

	return result, nil

}

func (b *BSONTable) colUnpack(v bson.RawElement, colType benchtop.FieldType) (any, error) {
	switch colType {
	case benchtop.String:
		if v.Value().Type != bson.TypeString {
			return nil, fmt.Errorf("expected String but got %s", v.Value().Type)
		}
		return v.Value().StringValue(), nil

	case benchtop.Double:
		if v.Value().Type != bson.TypeDouble {
			return nil, fmt.Errorf("expected Double but got %s", v.Value().Type)
		}
		return v.Value().Double(), nil

	case benchtop.Int64:
		if v.Value().Type != bson.TypeInt64 {
			return nil, fmt.Errorf("expected Int64 but got %s", v.Value().Type)
		}
		return v.Value().Int64(), nil

	case benchtop.Bytes:
		if v.Value().Type != bson.TypeBinary {
			return nil, fmt.Errorf("expected Binary but got %s", v.Value().Type)
		}
		binData, _ := v.Value().Binary()
		return binData, nil

	default:
		return nil, fmt.Errorf("unknown column type: %d", colType)
	}
}

func (b *BSONTable) getBlockPos(id []byte) (uint64, uint64, error) {
	val, closer, err := b.Pb.Db.Get(benchtop.NewPosKey(b.tableId, id))
	if err != nil {
		log.Errorln("getBlockPos Err: ", err)
		return 0, 0, err
	}
	offset, size := benchtop.ParsePosValue(val)
	defer closer.Close()
	return offset, size, nil
}

func (b *BSONTable) setDataIndices(inputs chan benchtop.Index) {
	for index := range inputs {
		b.addTableEntryInfo(nil, index.Key, index.Position, index.Size)
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
	var m bson.M
	bson.Unmarshal(rowData, &m)
	out, err := b.unpackData(false, m)
	if err != nil {
		return nil, err
	}
	return out.(map[string]any), nil
}

func (b *BSONTable) writeBsonEntry(offset int64, bData []byte) (int, error) {
	// make next offset equal to existing offset + length of data
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, uint64(offset)+uint64(len(bData))+8)
	_, err := b.handle.Write(buffer)
	if err != nil {
		return 0, fmt.Errorf("write offset error: %v", err)
	}
	n, err := b.handle.Write(bData)
	if err != nil {
		return 0, fmt.Errorf("write BSON error: %v", err)
	}
	return n, nil
}
