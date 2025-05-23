package bsontable

import (
	"bytes"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
)

const bufferSize = 100

// List all unique col names held by all tables
func (b *BSONDriver) GetAllColNames() chan string {
	out := make(chan string, bufferSize)
	go func() {
		defer close(out)
		prefix := []byte{benchtop.TablePrefix}
		b.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				info, err := b.getTableInfo(string(it.Key()))
				if err != nil {
					continue
				}
				for _, col := range info.Columns {
					out <- col.Key
				}
			}
			return nil
		})
	}()
	return out
}

func (b *BSONDriver) GetLabels(edges bool) chan string {
	out := make(chan string, bufferSize)
	go func() {
		defer close(out)
		prefix := []byte{benchtop.TablePrefix}
		b.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				strKey := string(benchtop.ParseTableKey(it.Key()))
				if (edges && strKey[:2] == "e_") || (!edges && strKey[:2] == "v_") {
					out <- strKey[2:]
				}
			}
			return nil
		})
	}()
	return out
}

func (b *BSONDriver) LoadTables(tType byte) {
	prefix := []byte{tType}
	b.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			table, _ := b.Get(string(it.Key()))
			b.Tables[string(it.Key())] = table.(*BSONTable)
		}
		return nil
	})
}
