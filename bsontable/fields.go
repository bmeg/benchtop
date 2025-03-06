package bsontable

import (
	"bytes"
	"strings"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
)

func (dr *BSONDriver) AddField(path string) error {
	fk := benchtop.FieldKey(path)
	dr.Fields[path] = strings.Split(path, ".")
	return dr.db.Set(fk, []byte{}, nil)
}

func (dr *BSONDriver) RemoveField(path string) error {
	fk := benchtop.FieldKey(path)
	delete(dr.Fields, path)
	return dr.db.Delete(fk, nil)
}

func (dr *BSONDriver) ListFields() []string {
	out := make([]string, 0, 10)
	fPrefix := benchtop.FieldPrefix
	dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(fPrefix); it.Valid() && bytes.HasPrefix(it.Key(), fPrefix); it.Next() {
			field := benchtop.FieldKeyParse(it.Key())
			out = append(out, field)
		}
		return nil
	})
	return out
}

func (dr *BSONDriver) GetIDsForLabel(label string) chan string {
	out := make(chan string, 10)
	go func() {
		defer close(out)
		table, err := dr.Get(label)
		if err != nil {
			log.Infof("GetIdsForLabel: %s on table: %s", err, label)
			return
		}

		rowsChan, err := table.Scan(true, nil)
		if err != nil {
			log.Errorf("Error scanning field %s: %s", label, err)
			return
		}

		for row := range rowsChan {
			if id, ok := row["_key"].(string); ok {
				out <- id
			}
		}
	}()
	return out
}
