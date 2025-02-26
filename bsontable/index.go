package bsontable

import (
	"bytes"

	"github.com/bmeg/benchtop"
	"github.com/cockroachdb/pebble"
)

const bufferSize = 1000

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
				out <- col.Name
			}
		}
		return

	}()
	return out
}

func (b *BSONDriver) LoadAllTables() {
	prefix := []byte{benchtop.TablePrefix}
	it, _ := b.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
		table, _ := b.Get(string(it.Key()))
		b.tables[string(it.Key())] = table.(*BSONTable)
	}
}
