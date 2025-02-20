package benchtop

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/bmeg/benchtop/util"
	"github.com/bmeg/grip/log"
	"github.com/cockroachdb/pebble"
	"go.mongodb.org/mongo-driver/bson"
)

type BSONDriver struct {
	base string
	db   *pebble.DB

	lock   sync.RWMutex
	tables map[string]*BSONTable
}

func NewBSONDriver(path string) (TableDriver, error) {
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

func (dr *BSONDriver) New(name string, columns []ColumnDef) (TableStore, error) {

	p, _ := dr.Get(name)
	if p != nil {
		return p, fmt.Errorf("table %s already exists", name)
	}

	dr.lock.Lock()
	defer dr.lock.Unlock()

	// Prepend Key column to columns provided by user
	columns = append([]ColumnDef{{Name: "keyName", Type: String}}, columns...)

	tPath := filepath.Join(dr.base, "TABLES", name)
	out := &BSONTable{columns: columns,
		handleLock: sync.RWMutex{}, columnMap: map[string]int{},
		path: tPath}
	f, err := os.Create(tPath)
	if err != nil {
		return nil, err
	}
	out.handle = f
	for n, d := range columns {
		out.columnMap[d.Name] = n
	}

	newID := dr.getMaxTableID() + 1

	if err := dr.addTableID(newID, name); err != nil {
		log.Errorf("Error: %s", err)
	}
	if err := dr.addTableEntry(newID, name, columns); err != nil {
		log.Errorf("Error: %s", err)
	}
	out.db = dr.db
	out.tableId = newID
	dr.tables[name] = out
	return out, nil
}

func (dr *BSONDriver) List() []string {
	out := []string{}
	prefix := []byte{namePrefix}
	it, _ := dr.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
		value := ParseNameKey(it.Key())
		out = append(out, string(value))
	}
	it.Close()
	return out
}


func (dr *BSONDriver) Close() {
	log.Infoln("Closing driver")
	for _, i := range dr.tables {
		i.handle.Close()
	}
	dr.db.Close()
}

func (dr *BSONDriver) Get(name string) (TableStore, error) {
	dr.lock.Lock()
	defer dr.lock.Unlock()

	if x, ok := dr.tables[name]; ok {
		return x, nil
	}

	nkey := NewNameKey([]byte(name))
	value, closer, err := dr.db.Get(nkey)
	if err != nil {
		return nil, err
	}
	tinfo := TableInfo{}
	bson.Unmarshal(value, &tinfo)
	closer.Close()

	tPath := filepath.Join(dr.base, "TABLES", name)

	f, err := os.Open(tPath)
	if err != nil {
		return nil, err
	}

	out := &BSONTable{
		columns: tinfo.Columns,
		db:      dr.db,
		tableId: tinfo.Id,
		handle:  f,
		path:    tPath,
	}
	dr.tables[name] = out

	return out, nil
}
