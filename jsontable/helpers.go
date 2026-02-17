package jsontable

import (
	"bytes"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
)

// Specify a table type prefix to differentiate between edge tables and vertex tables
func (dr *JSONDriver) getMaxTablePrefix() uint16 {
	// Note: Caller must hold dr.Lock

	prefix := []byte{benchtop.TablePrefix}
	// Reserve 0 as invalid/unset; real table IDs start at 1.
	maxID := uint16(1)
	dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			val, err := it.Value()
			if err != nil {
				continue
			}
			var tinfo benchtop.TableInfo
			if err := sonic.Unmarshal(val, &tinfo); err == nil {
				if tinfo.TableId >= maxID {
					maxID = tinfo.TableId + 1
				}
			}
		}
		return nil
	})

	return maxID
}

func (dr *JSONDriver) addTable(Name string, TinfoMarshal []byte) error {
	nkey := benchtop.NewTableKey([]byte(Name))
	return dr.Pkv.Set(nkey, TinfoMarshal, nil)
}

func (dr *JSONDriver) dropTable(name string) error {
	nkey := benchtop.NewTableKey([]byte(name))
	return dr.Pkv.Delete(nkey, nil)

}

func (dr *JSONDriver) getTableInfo(name string) (benchtop.TableInfo, error) {
	value, closer, err := dr.Pkv.Get([]byte(name))
	if err != nil {
		return benchtop.TableInfo{}, err
	}
	tinfo := benchtop.TableInfo{}
	sonic.ConfigFastest.Unmarshal(value, &tinfo)
	closer.Close()
	return tinfo, nil
}

func (dr *JSONDriver) AddTableEntryInfo(tx *pebblebulk.PebbleBulk, rowId []byte, rowLoc *benchtop.RowLoc) error {
	value := benchtop.EncodeRowLoc(rowLoc)
	posKey := benchtop.NewPosKey(rowLoc.TableId, rowId)
	if tx != nil {
		err := tx.Set(posKey, value, nil)
		if err != nil {
			return err
		}
	} else {
		err := dr.Pkv.Set(posKey, value, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dr *JSONDriver) GetLocFromTableKey(id []byte) (loc *benchtop.RowLoc, err error) {
	val, closer, err := dr.Pkv.Get(benchtop.NewPosKey(loc.TableId, id))
	if err != nil {
		if err != pebble.ErrNotFound {
			log.Errorln("GetLocFromTableKey Err: ", err)
		}
		return nil, err
	}
	defer closer.Close()
	return benchtop.DecodeRowLoc(val), nil
}
