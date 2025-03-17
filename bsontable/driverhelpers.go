package bsontable

import (
	"bytes"

	"github.com/bmeg/benchtop"
	"github.com/cockroachdb/pebble"
	"go.mongodb.org/mongo-driver/bson"
)

// Specify a table type prefix to differentiate between edge tables and vertex tables
func (dr *BSONDriver) getMaxTablePrefix() uint32 {
	// get the max table uint32. Useful for fetching keys.
	prefix := []byte{benchtop.TablePrefix}
	it, _ := dr.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	maxID := uint32(0)
	for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
		maxID++
	}
	it.Close()
	return maxID
}

func (dr *BSONDriver) addTable(id uint32, name string, columns []benchtop.ColumnDef, fileName string) error {
	tdata, _ := bson.Marshal(benchtop.TableInfo{Columns: columns, Id: id, FileName: fileName})
	nkey := benchtop.NewTableKey([]byte(name))
	return dr.db.Set(nkey, tdata, nil)
}

func (dr *BSONDriver) getTableInfo(name string) (benchtop.TableInfo, error) {
	value, closer, err := dr.db.Get([]byte(name))
	if err != nil {
		return benchtop.TableInfo{}, err
	}
	tinfo := benchtop.TableInfo{}
	bson.Unmarshal(value, &tinfo)
	closer.Close()
	return tinfo, nil
}
