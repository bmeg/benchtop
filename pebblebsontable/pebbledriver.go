package pebblebsontable

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
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
	columns   []benchtop.ColumnDef
	columnMap map[string]int
	db        *pebble.DB
	TableId   uint32
}

func NewPebbleBSONDriver(path string) (benchtop.TableDriver, error) {
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

func (dr *PebbleBSONDriver) Get(name string) (benchtop.TableStore, error) {
	dr.lock.Lock()
	defer dr.lock.Unlock()

	if x, ok := dr.tables[name]; ok {
		return x, nil
	}

	nkey := benchtop.NewTableEntryKey([]byte(name))
	value, closer, err := dr.db.Get(nkey)
	if err != nil {
		return nil, err
	}
	tinfo := benchtop.TableInfo{}
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

func (dr *PebbleBSONDriver) addTableEntry(id uint32, name string, columns []benchtop.ColumnDef) error {
	tdata, _ := bson.Marshal(benchtop.TableInfo{Columns: columns, Id: id})
	nkey := benchtop.NewTableEntryKey([]byte(name))
	return dr.db.Set(nkey, tdata, nil)
}

func (dr *PebbleBSONDriver) addTableID(newID uint32, name string) error {
	idKey := benchtop.NewTableIdKey(newID)
	return dr.db.Set(idKey, []byte(name), nil)
}

func (dr *PebbleBSONDriver) New(name string, columns []benchtop.ColumnDef) (benchtop.TableStore, error) {

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
	prefix := []byte{benchtop.EntryPrefix}
	it, _ := dr.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
		value := benchtop.ParseTableEntryKey(it.Key())
		out = append(out, string(value))
	}
	it.Close()
	return out
}

func (b *PebbleBSONTable) Close() {
}

func (b *PebbleBSONTable) GetColumns() []benchtop.ColumnDef {
	return b.columns
}

func (b *PebbleBSONTable) packData(entry map[string]any) (bson.D, error) {
	// pack named columns
	columns := []any{}
	for _, c := range b.columns {
		if e, ok := entry[c.Name]; ok {
			v, err := benchtop.CheckType(e, c.Type)
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
	key := append(benchtop.NewPosKeyPrefix(b.TableId), id...)

	err = b.db.Set(key, bData, &pebble.WriteOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (b *PebbleBSONTable) Get(id []byte, fields ...string) (map[string]any, error) {

	key := append(benchtop.NewPosKeyPrefix(b.TableId), id...)
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

func (b *PebbleBSONTable) colUnpack(v bson.RawElement, colType benchtop.FieldType) any {
	if colType == benchtop.String {
		return v.Value().StringValue()
	} else if colType == benchtop.Double {
		return v.Value().Double()
	} else if colType == benchtop.Int64 {
		return v.Value().Int64()
	} else if colType == benchtop.Bytes {
		_, data := v.Value().Binary()
		return data
	}
	return nil
}

func (b *PebbleBSONTable) Fetch(inputs chan benchtop.Index, workers int) <-chan benchtop.BulkResponse {
	panic("not implemented")
}

func (b *PebbleBSONTable) Remove(inputs chan benchtop.Index, workers int) <-chan benchtop.BulkResponse {
	panic("not implemented")
}

func (b *PebbleBSONTable) Keys() (chan benchtop.Index, error) {
	out := make(chan benchtop.Index, 10)
	go func() {
		defer close(out)

		prefix := benchtop.NewPosKeyPrefix(b.TableId)
		it, err := b.db.NewIter(&pebble.IterOptions{})
		if err != nil {
			log.Printf("error: %s", err)
		}
		for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			_, value := benchtop.ParsePosKey(it.Key())
			out <- benchtop.Index{Key: value}
		}
		it.Close()
	}()
	return out, nil
}

func (b *PebbleBSONTable) Scan(key bool, filter []benchtop.FieldFilter, fields ...string) (chan map[string]any, error) {
	return nil, nil
}

func (b *PebbleBSONTable) Load(inputs chan benchtop.Entry) error {

	b.bulkWrite(func(s benchtop.DbSet) error {
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

func (b *PebbleBSONTable) bulkWrite(u func(s benchtop.DbSet) error) error {
	batch := b.db.NewBatch()
	ptx := &pebblebulk.PebbleBulkWrite{Db: b.db, Batch: batch, Lowest: nil, Highest: nil, CurSize: 0}
	err := u(ptx)
	batch.Commit(nil)
	batch.Close()
	if ptx.Lowest != nil && ptx.Highest != nil {
		b.db.Compact(ptx.Lowest, ptx.Highest, true)
	}
	return err
}

func (b *PebbleBSONTable) Delete(name []byte) error {
	posKey := benchtop.NewPosKey(b.TableId, name)
	b.db.Delete(posKey, nil)
	return nil
}

func (b *PebbleBSONTable) Compact() error {
	return nil
}
