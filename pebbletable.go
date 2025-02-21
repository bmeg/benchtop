package benchtop

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/bmeg/benchtop/util"
	"github.com/cockroachdb/pebble"

	"go.mongodb.org/mongo-driver/bson"
)

type PebbleBSONDriver struct {
	base   string
	db     *pebble.DB
	lock   sync.Mutex
	tables map[string]*PebbleBSONTable
}

type PebbleBSONTable struct {
	columns   []ColumnDef
	columnMap map[string]int
	db        *pebble.DB
	TableId   uint32
}

func NewPebbleBSONDriver(path string) (TableDriver, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	tableDir := filepath.Join(path, "TABLES")
	if util.FileExists(tableDir) {
		os.Mkdir(tableDir, 0700)
	}
	return &PebbleBSONDriver{base: path, db: db, tables: map[string]*PebbleBSONTable{}}, nil
}

func (dr *PebbleBSONDriver) Close() {
	log.Println("Closing driver")
	dr.db.Close()
}

func (dr *PebbleBSONDriver) Get(name string) (TableStore, error) {
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
	defer closer.Close()

	out := &PebbleBSONTable{
		columns: tinfo.Columns,
		db:      dr.db,
		TableId: tinfo.Id,
	}
	dr.tables[name] = out

	return out, nil
}

func (dr *PebbleBSONDriver) getMaxTableID() uint32 {
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

func (dr *PebbleBSONDriver) addTableEntry(id uint32, name string, columns []ColumnDef) error {
	tdata, _ := bson.Marshal(TableInfo{Columns: columns, Id: id})
	nkey := NewNameKey([]byte(name))
	return dr.db.Set(nkey, tdata, nil)
}

func (dr *PebbleBSONDriver) addTableID(newID uint32, name string) error {
	idKey := NewIDKey(newID)
	return dr.db.Set(idKey, []byte(name), nil)
}

func (dr *PebbleBSONDriver) New(name string, columns []ColumnDef) (TableStore, error) {

	p, _ := dr.Get(name)
	if p != nil {
		return p, fmt.Errorf("table %s already exists", name)
	}

	dr.lock.Lock()
	defer dr.lock.Unlock()

	out := &PebbleBSONTable{columns: columns, columnMap: map[string]int{}}

	for n, d := range columns {
		out.columnMap[d.Name] = n
	}

	newID := dr.getMaxTableID() + 1

	if err := dr.addTableID(newID, name); err != nil {
		log.Printf("Error: %s", err)
	}
	if err := dr.addTableEntry(newID, name, columns); err != nil {
		log.Printf("Error: %s", err)
	}
	out.db = dr.db
	out.TableId = newID
	dr.tables[name] = out
	return out, nil
}

func (dr *PebbleBSONDriver) List() []string {
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

func (b *PebbleBSONTable) Close() {
}

func (b *PebbleBSONTable) GetColumns() []ColumnDef {
	return b.columns
}

func (b *PebbleBSONTable) packData(entry map[string]any) (bson.D, error) {
	// pack named columns
	columns := []any{}
	for _, c := range b.columns {
		if e, ok := entry[c.Name]; ok {
			v, err := checkType(e, c.Type)
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
	return bson.D{{Key: "columns", Value: columns}, {Key: "data", Value: other}}, nil
}

func (b *PebbleBSONTable) Add(id []byte, entry map[string]any) error {
	dData, err := b.packData(entry)
	if err != nil {
		return err
	}
	bData, err := bson.Marshal(dData)
	if err != nil {
		return err
	}
	key := append(NewPosKeyPrefix(b.TableId), id...)

	err = b.db.Set(key, bData, &pebble.WriteOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (b *PebbleBSONTable) Get(id []byte, fields ...string) (map[string]any, error) {

	key := append(NewPosKeyPrefix(b.TableId), id...)
	rowData, closer, err := b.db.Get(key)
	if err != nil {
		fmt.Println("ERR: ", err)
	}

	bd := bson.Raw(rowData)
	defer closer.Close()

	columns := bd.Index(0).Value().Array()
	out := map[string]any{}
	if len(fields) == 0 {
		if err := bd.Index(1).Value().Unmarshal(&out); err != nil {
			return nil, err
		}
		elem, err := columns.Elements()
		if err != nil {
			return nil, err
		}
		for i, n := range b.columns {
			out[n.Name] = b.colUnpack(elem[i], n.Type)
		}
	} else {
		for _, colName := range fields {
			if i, ok := b.columnMap[colName]; ok {
				n := b.columns[i]
				elem := columns.Index(uint(i))
				out[n.Name] = b.colUnpack(elem, n.Type)
			}
		}
	}
	return out, nil
}

func (b *PebbleBSONTable) colUnpack(v bson.RawElement, colType FieldType) any {
	if colType == String {
		return v.Value().StringValue()
	} else if colType == Double {
		return v.Value().Double()
	} else if colType == Int64 {
		return v.Value().Int64()
	} else if colType == Bytes {
		_, data := v.Value().Binary()
		return data
	}
	return nil
}

func (b *PebbleBSONTable) Fetch(inputs chan Index, workers int) <-chan BulkResponse {
	panic("not implemented")
}

func (b *PebbleBSONTable) Remove(inputs chan Index, workers int) <-chan BulkResponse {
	panic("not implemented")
}

func (b *PebbleBSONTable) Keys() (chan Index, error) {
	out := make(chan Index, 10)
	go func() {
		defer close(out)

		prefix := NewPosKeyPrefix(b.TableId)
		it, err := b.db.NewIter(&pebble.IterOptions{})
		if err != nil {
			log.Printf("error: %s", err)
		}
		for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			_, value := ParsePosKey(it.Key())
			out <- Index{Key: value}
		}
		it.Close()
	}()
	return out, nil
}

func (b *PebbleBSONTable) Scan(key bool, filter []FieldFilter, fields ...string) (chan map[string]any, error) {
	return nil, nil
}

func (b *PebbleBSONTable) Load(inputs chan Entry) error {

	b.bulkWrite(func(s dbSet) error {
		for entry := range inputs {
			dData, err := b.packData(entry.Value)
			if err != nil {
				return err
			}
			bData, err := bson.Marshal(dData)
			if err != nil {
				return err
			}
			b.db.Set(entry.Key, bData, &pebble.WriteOptions{})

		}
		return nil
	})

	return nil
}

func (b *PebbleBSONTable) bulkWrite(u func(s dbSet) error) error {
	batch := b.db.NewBatch()
	ptx := &pebbleBulkWrite{b.db, batch, nil, nil, 0}
	err := u(ptx)
	batch.Commit(nil)
	batch.Close()
	if ptx.lowest != nil && ptx.highest != nil {
		b.db.Compact(ptx.lowest, ptx.highest, true)
	}
	return err
}

func (b *PebbleBSONTable) Delete(name []byte) error {
	posKey := NewPosKey(b.TableId, name)
	b.db.Delete(posKey, nil)
	return nil
}

func (b *PebbleBSONTable) Compact() error {
	return nil
}
