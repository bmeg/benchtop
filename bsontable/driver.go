package bsontable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/benchtop/util"
	"github.com/bmeg/grip/log"
	"github.com/cockroachdb/pebble"
	"go.mongodb.org/mongo-driver/bson"
)

type BSONDriver struct {
	base   string
	lock   sync.RWMutex
	db     *pebble.DB
	Pb     *pebblebulk.PebbleBulk
	Tables map[string]*BSONTable
}

func NewBSONDriver(path string) (benchtop.TableDriver, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	tableDir := filepath.Join(path, "TABLES")
	if util.FileExists(tableDir) {
		os.Mkdir(tableDir, 0700)
	}
	return &BSONDriver{
		base:   path,
		db:     db,
		Tables: map[string]*BSONTable{},
	}, nil
}

func (dr *BSONDriver) New(name string, columns []benchtop.ColumnDef) (benchtop.TableStore, error) {
	p, _ := dr.Get(name)
	if p != nil {
		return p, fmt.Errorf("table %s already exists", name)
	}

	dr.lock.Lock()
	defer dr.lock.Unlock()

	tPath := filepath.Join(dr.base, "TABLES", name)
	out := &BSONTable{
		columns:    columns,
		handleLock: sync.RWMutex{},
		columnMap:  map[string]int{},
		path:       tPath,
	}
	f, err := os.Create(tPath)
	if err != nil {
		return nil, err
	}
	out.handle = f
	for n, d := range columns {
		out.columnMap[d.Name] = n
	}

	outData, err := bson.Marshal(out)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, uint64(0)+uint64(len(outData))+8)
	out.handle.Write(buffer)
	out.handle.Write(outData)

	newId := dr.getMaxTablePrefix()
	if err := dr.addTable(newId, name, columns); err != nil {
		log.Errorf("Error: %s", err)
	}
	out.db = dr.db
	out.Pb = &pebblebulk.PebbleBulk{Db: dr.db}
	out.tableId = newId
	dr.Tables[name] = out
	return out, nil
}

func (dr *BSONDriver) List() []string {
	out := []string{}
	prefix := []byte{benchtop.TablePrefix}
	it, _ := dr.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
		value := benchtop.ParseTableKey(it.Key())
		out = append(out, string(value))
	}
	it.Close()
	return out
}

func (dr *BSONDriver) Close() {
	log.Infoln("Closing driver")
	for _, i := range dr.Tables {
		i.handle.Close()
	}
	dr.db.Close()
}

func (dr *BSONDriver) Get(name string) (benchtop.TableStore, error) {
	dr.lock.Lock()
	defer dr.lock.Unlock()

	if x, ok := dr.Tables[name]; ok {
		return x, nil
	}

	nkey := benchtop.NewTableKey([]byte(name))
	value, closer, err := dr.db.Get(nkey)
	if err != nil {
		return nil, err
	}
	tinfo := benchtop.TableInfo{}
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
	dr.Tables[name] = out

	return out, nil
}

func (dr *BSONDriver) Delete(name string) error {
	dr.lock.Lock()
	defer dr.lock.Unlock()

	table, exists := dr.Tables[name]
	if !exists {
		return fmt.Errorf("table %s does not exist", name)
	}

	table.handleLock.Lock()
	defer table.handleLock.Unlock()

	if table.handle != nil {
		if err := table.handle.Close(); err != nil {
			log.Errorf("Error closing table %s handle: %v", name, err)
		}
	}

	tPath := filepath.Join(dr.base, "TABLES", name)
	if err := os.Remove(tPath); err != nil {
		return fmt.Errorf("failed to delete table file %s: %v", tPath, err)
	}
	delete(dr.Tables, name)

	return nil
}
