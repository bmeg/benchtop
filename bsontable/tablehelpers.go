package bsontable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable/tpath"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/jsonpath"
	"github.com/cockroachdb/pebble"
	"go.mongodb.org/mongo-driver/bson"
)

type RowData struct {
	Data map[string]any 	`json:"0"`
	Key  string				`json:"1"`
}

func (b *BSONTable) packData(entry map[string]any, key string) *RowData {
	return &RowData{
        Data:   	entry,
        Key:     	key,
    }
}

func (b *BSONTable) AddTableEntryInfo(tx *pebblebulk.PebbleBulk, rowId []byte, rowLoc benchtop.RowLoc) {
	value := benchtop.NewPosValue(rowLoc.Offset, rowLoc.Size)
	posKey := benchtop.NewPosKey(b.TableId, rowId)
	if tx != nil {
		tx.Set(posKey, value, nil)
	} else {
		b.db.Set(posKey, value, nil)
	}
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

func (b *BSONTable) getTableEntryInfo(snap *pebble.Snapshot, id []byte) (*benchtop.RowLoc, error) {
	// Really only want to see if anything was returned or not
	_, closer, err := snap.Get(benchtop.NewPosKey(b.TableId, id))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return &benchtop.RowLoc{}, nil
}

func (b *BSONTable) unpackData(justKeys bool, retId bool, doc *RowData) (any, error) {
	if doc == nil {
		return nil, fmt.Errorf("Doc is nil nothing to unpack")
	}
	if justKeys {
		return doc.Key, nil
	}
	if retId && doc.Data != nil{
		doc.Data["_id"] = doc.Key
	}
	return doc.Data, nil

}

func (b *BSONTable) GetBlockPos(id []byte) (offset uint64, size uint64, err error) {
	log.Debugln("TABLE ID: ", b.TableId, "ID: ", string(id))
	val, closer, err := b.db.Get(benchtop.NewPosKey(b.TableId, id))
	if err != nil {
		if err != pebble.ErrNotFound {
			log.Errorln("getBlockPos Err: ", err)
		}
		return 0, 0, err
	}

	offset, size = benchtop.ParsePosValue(val)
	defer closer.Close()
	return offset, size, nil
}

func (b *BSONTable) setDataIndices(inputs chan benchtop.Index) {
	for index := range inputs {
		b.AddTableEntryInfo(
			nil,
			index.Key,
			benchtop.RowLoc{
				Offset: index.Position,
				Size:   index.Size,
			},
		)
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
	var m *RowData = nil
	bson.Unmarshal(rowData, m)
	out, err := b.unpackData(false, false, m)
	if err != nil {
		return nil, err
	}
	return out.(map[string]any), nil
}

func (b *BSONTable) writeBsonEntry(offset int64, bData []byte) (int, error) {
	// make next offset equal to existing offset + length of data
	buffer := make([]byte, 12)
	binary.LittleEndian.PutUint64(buffer[:8], uint64(offset)+uint64(len(bData))+12)
	binary.LittleEndian.PutUint32(buffer[8:], uint32(len(bData)))

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
