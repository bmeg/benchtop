package benchtop

import (
	"bytes"

	"github.com/cockroachdb/pebble"
	"go.mongodb.org/mongo-driver/bson"
)

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
