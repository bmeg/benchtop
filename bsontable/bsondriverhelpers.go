package bsontable

import (
	"bytes"

	"github.com/bmeg/benchtop"
	"github.com/cockroachdb/pebble"
	"go.mongodb.org/mongo-driver/bson"
)

func (dr *BSONDriver) getMaxTableID() uint32 {
	// get unique id
	prefix := []byte{benchtop.TablePrefix}
	it, _ := dr.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	maxID := uint32(0)
	for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
		value := benchtop.ParseTableIDKey(it.Key())
		maxID = value
	}
	it.Close()
	return maxID
}

func (dr *BSONDriver) addTableEntry(id uint32, name string, columns []benchtop.ColumnDef) error {
	tdata, _ := bson.Marshal(benchtop.TableInfo{Columns: columns, Id: id})

	nkey := benchtop.NewTableEntryKey([]byte(name))

	return dr.db.Set(nkey, tdata, nil)
}

func (dr *BSONDriver) addTableID(newID uint32, name string) error {
	idKey := benchtop.NewTableIdKey(newID)
	return dr.db.Set(idKey, []byte(name), nil)
}
