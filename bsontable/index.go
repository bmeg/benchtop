package bsontable

import (
	"bytes"

	"github.com/bmeg/benchtop"
	"github.com/cockroachdb/pebble"
)

const bufferSize = 100

// List all unique col names held by all tables
func (b *BSONDriver) GetAllColNames() chan string {
	out := make(chan string, bufferSize)
	go func() {
		defer close(out)
		prefix := []byte{benchtop.TablePrefix}
		it, _ := b.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
		for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			info, err := b.getTableInfo(string(it.Key()))
			if err != nil {
				continue
			}
			for _, col := range info.Columns {
				out <- col.Key
			}
		}
		return

	}()
	return out
}

func (b *BSONDriver) GetLabels(edges bool) chan string {
	out := make(chan string, bufferSize)
	go func() {
		defer close(out)
		prefix := []byte{benchtop.TablePrefix}
		it, _ := b.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
		for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			strKey := string(benchtop.ParseTableKey(it.Key()))
			if (edges && strKey[:2] == "e_") || (!edges && strKey[:2] == "v_") {
				out <- strKey[2:]
			}
		}
		return
	}()
	return out
}

func (b *BSONDriver) LoadTables(tType byte) {
	prefix := []byte{tType}
	it, _ := b.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
		table, _ := b.Get(string(it.Key()))
		b.Tables[string(it.Key())] = table.(*BSONTable)
	}
}
