package bsontable

import (
	"bytes"
	"fmt"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
	"go.mongodb.org/mongo-driver/bson"
)

func (dr *BSONDriver) AddIndex(field string, value any, label string, rowId []byte) error {
	/* Add Index expects that a field has been added already so if it doesn't exist it will err */
	if _, exists := dr.Fields[label][field]; exists == false {
		return fmt.Errorf("Index label '%s' and field '%s' does not exist", label, field)
	}
	return dr.db.Set(
		benchtop.FieldKey(label, field, value, rowId),
		[]byte{},
		nil,
	)
}

func (dr *BSONDriver) AddField(label, field string) error {
	if _, exists := dr.Fields[label][field]; exists == false {
		dr.Fields[label][field] = struct{}{}
	} else {
		return fmt.Errorf("index label '%s' field '%s' already exists", label, field)
	}
	return dr.db.Set(
		benchtop.FieldKey(label, field, nil, nil),
		[]byte{},
		nil,
	)
}

func (dr *BSONDriver) RemoveField(label, field string) error {
	delete(dr.Fields[label], field)
	delete(dr.Fields, label)
	return dr.db.Delete(
		benchtop.FieldKey(label, field, nil, nil),
		nil,
	)
}

func (dr *BSONDriver) RemoveIndex(field string, value any, label string, rowId []byte) error {
	delete(dr.Fields[label], field)
	delete(dr.Fields, label)
	return dr.db.Delete(
		benchtop.FieldKey(label, field, value, rowId),
		nil,
	)
}

func (dr *BSONDriver) LoadFields() {
	fPrefix := benchtop.FieldPrefix
	dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(fPrefix); it.Valid() && bytes.HasPrefix(it.Key(), fPrefix); it.Next() {
			label, field, _, _ := benchtop.FieldKeyParse(it.Key())
			dr.Fields[label] = make(map[string]struct{})
			dr.Fields[label][field] = struct{}{}
		}
		return nil
	})
}

type FieldInfo struct {
	Label string
	Field string
}

func (dr *BSONDriver) ListFields() []FieldInfo {
	seenFields := make(map[string]map[string]struct{})
	fPrefix := benchtop.FieldPrefix
	var out []FieldInfo
	dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(fPrefix); it.Valid() && bytes.HasPrefix(it.Key(), fPrefix); it.Next() {
			label, field, _, _ := benchtop.FieldKeyParse(it.Key())
			if _, exists := seenFields[label]; !exists {
				seenFields[label] = make(map[string]struct{})
				if _, exists := seenFields[label][field]; !exists {
					out = append(out, FieldInfo{Label: label, Field: field})
					seenFields[label][field] = struct{}{}
				}
			}
		}
		return nil
	})
	return out
}

func (dr *BSONDriver) RowIdsByFieldValue(field string, value any) (chan string, error) {
	valueBytes, err := bson.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal value: %v", err)
	}
	prefix := bytes.Join([][]byte{
		benchtop.FieldPrefix,
		[]byte(field),
		valueBytes,
	}, benchtop.FieldSep)

	out := make(chan string, 100)
	go func() {
		defer close(out)
		dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				parts := bytes.Split(it.Key(), benchtop.FieldSep)
				rowID := make([]byte, len(parts[4]))
				copy(rowID, parts[4])
				out <- string(rowID)
			}
			return nil
		})
		return
	}()
	return out, nil
}

func (dr *BSONDriver) RowIdsByLabelFieldValue(label string, field string, value any) (chan string, error) {
	valueBytes, err := bson.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal value: %v", err)
	}

	prefix := bytes.Join([][]byte{
		benchtop.FieldPrefix,
		[]byte(label),
		[]byte(field),
		valueBytes,
	}, benchtop.FieldSep)

	out := make(chan string, 100)
	go func() {
		defer close(out)
		dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				out <- string(bytes.Split(it.Key(), benchtop.FieldSep)[4])
			}
			return nil
		})
		return
	}()
	return out, nil
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
