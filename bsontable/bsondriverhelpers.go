package bsontable

import (
	"bytes"
	"os"
	"path/filepath"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/util"
	"github.com/cockroachdb/pebble"
	"go.mongodb.org/mongo-driver/bson"
)

func NewBSONDriver(path string) (benchtop.TableDriver, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	tableDir := filepath.Join(path, "TABLES")
	if util.FileExists(tableDir) {
		os.Mkdir(tableDir, 0700)
	}
	return &BSONDriver{base: path, db: db, tables: map[string]*BSONTable{}}, nil
}

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

func (dr *BSONDriver) addTable(id uint32, name string, columns []benchtop.ColumnDef) error {
	tdata, _ := bson.Marshal(benchtop.TableInfo{Columns: columns, Id: id})
	nkey := benchtop.NewTableKey([]byte(name))
	return dr.db.Set(nkey, tdata, nil)
}
