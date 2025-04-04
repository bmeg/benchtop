package bsontable

import (
	"bytes"
	"strings"

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

func (dr *BSONDriver) GetValuesForTableField(tableName, field string) ([]string, error) {
	prefix := []byte(string(benchtop.FieldPrefix) + tableName + ":" + field + ":")
	values := make(map[string]struct{})
	err := dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			key := string(it.Key())
			parts := strings.SplitN(key[len(prefix):], ":", 2) // Split after prefix
			if len(parts) > 1 {
				values[parts[0]] = struct{}{} // Value is first part
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var result []string
	for val := range values {
		result = append(result, val)
	}
	return result, nil
}

func (dr *BSONDriver) GetIDsForTableField(tableName, field string) ([]string, error) {
	// Prefix includes table and field, e.g., "Fv_Observation:component[*].code.coding[*].userSelected:"
	prefix := []byte(string(benchtop.FieldPrefix) + tableName + ":" + field + ":")
	var ids []string
	err := dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			key := it.Key()
			lastColon := bytes.LastIndex(key, []byte(":"))
			if lastColon != -1 && lastColon < len(key)-1 {
				id := string(key[lastColon+1:])
				ids = append(ids, id)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}
