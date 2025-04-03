package bsontable

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	multierror "github.com/hashicorp/go-multierror"
)

func (dr *BSONDriver) BulkAddField(idxChan <-chan *gripql.IndexID) error {
	err := dr.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		var bulkErr *multierror.Error
		for idx := range idxChan {
			path := fmt.Sprintf("%s.%s", idx.Label, idx.Field)
			fk := benchtop.FieldKey(path)
			// TODO: need to load this from disk on db startup
			dr.Fields[path] = strings.Split(path, ".")
			err := tx.Set(fk, []byte{}, nil)
			if err != nil {
				bulkErr = multierror.Append(bulkErr, fmt.Errorf("failed to set index for %s: %v", path, err))
			}
		}
		return bulkErr.ErrorOrNil()
	})
	return err
}

func (dr *BSONDriver) AddFieldIndex(path string) error {
	fk := benchtop.FieldKey(path)
	dr.Fields[path] = strings.Split(path, ".")
	return dr.db.Set(fk, []byte{}, nil)
}

func (dr *BSONDriver) RemoveFieldIndex(path string) error {
	fk := benchtop.FieldKey(path)
	delete(dr.Fields, path)
	return dr.db.Delete(fk, nil)
}

func (dr *BSONDriver) ListFields(graphName string) <-chan *gripql.IndexID {
	fPrefix := benchtop.FieldPrefix
	out := make(chan *gripql.IndexID, 10)
	go func() {
		defer close(out)
		err := dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(fPrefix); it.Valid() && bytes.HasPrefix(it.Key(), fPrefix); it.Next() {
				t := strings.Split(benchtop.FieldKeyParse(it.Key()), ".")
				out <- &gripql.IndexID{Graph: graphName, Label: t[0], Field: strings.Join(t[1:], ".")}

			}
			return nil
		})
		if err != nil {
			// Optionally log the error since we can't return it directly
			log.Errorf("Error listing fields: %v", err)
		}
	}()
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
