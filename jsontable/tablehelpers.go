package jsontable

import (
	"fmt"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/tpath"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/jsonpath"
	"github.com/cockroachdb/pebble"
)

type RowData struct {
	Data map[string]any `json:"0"`
	Key  string         `json:"1"`
}

func (b *JSONTable) packData(entry map[string]any, key string) *RowData {
	return &RowData{
		Data: entry,
		Key:  key,
	}
}

func (b *JSONTable) getTableEntryInfo(snap *pebble.Snapshot, id []byte) (*benchtop.RowLoc, error) {
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

func (b *JSONTable) AddTableEntryInfo(tx *pebblebulk.PebbleBulk, rowId []byte, rowLoc *benchtop.RowLoc) error {
	value := benchtop.EncodeRowLoc(rowLoc)
	posKey := benchtop.NewPosKey(b.TableId, rowId)
	if tx != nil {
		err := tx.Set(posKey, value, nil)
		if err != nil {
			return err
		}
	} else {
		err := b.Pb.Db.Set(posKey, value, nil)
		if err != nil {
			return err
		}
	}
	return nil
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

func (b *JSONTable) unpackData(loadData bool, retId bool, doc *RowData) (any, error) {
	if doc == nil {
		return nil, fmt.Errorf("Doc is nil nothing to unpack")
	}
	if !loadData {
		return doc.Key, nil
	}
	if retId && doc.Data != nil {
		doc.Data["_id"] = doc.Key
	}
	return doc.Data, nil

}

func (b *JSONTable) GetBlockPos(id []byte) (loc *benchtop.RowLoc, err error) {
	val, closer, err := b.db.Get(benchtop.NewPosKey(b.TableId, id))
	if err != nil {
		if err != pebble.ErrNotFound {
			log.Errorln("getBlockPos Err: ", err)
		}
		return nil, err
	}
	defer closer.Close()
	return benchtop.DecodeRowLoc(val), nil
}

func (b *JSONTable) setDataIndices(inputs chan benchtop.Index) {
	b.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		for index := range inputs {
			b.AddTableEntryInfo(
				tx,
				index.Key,
				&benchtop.RowLoc{
					Offset: index.Loc.Offset,
					Size:   index.Loc.Size,
				},
			)
		}
		return nil
	})
}
