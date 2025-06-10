package bsontable

import (
	"bytes"
	"fmt"

	"github.com/bmeg/benchtop"

	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/cockroachdb/pebble"
	tableFilters "github.com/bmeg/benchtop/bsontable/filters"
	"github.com/bmeg/grip/log"
)

func (dr *BSONDriver) AddField(label, field string) error {
	dr.Lock.RLock()
	defer dr.Lock.RUnlock()
	foundTable, ok := dr.Tables[label]
	if !ok {
		log.Debugf("Creating index for table '%s' that has not been written yet", label)
		// If the table doesn't yet exist, write the index Key stub.
		err := dr.db.Set(
			benchtop.FieldKey(field, label, nil, nil),
			[]byte{},
			nil,
		)
		if err != nil {
			return err
		}
	} else {
		log.Debugf("Found table %s writing indices for field %s", label, field)
		rowChan, err := foundTable.Scan(true, nil)
		if err != nil {
			return err
		}
		err = dr.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
			for r := range rowChan {
				err := tx.Set(benchtop.FieldKey(field, label, PathLookup(r, field), []byte(r["_key"].(string))),
					[]byte{},
					nil,
				)
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	innerMap, existsLabel := dr.Fields[label]
	if !existsLabel {
		innerMap = make(map[string]struct{})
		dr.Fields[label] = innerMap
	}
	if _, existsField := innerMap[field]; existsField {
		return fmt.Errorf("index label '%s' field '%s' already exists", label, field)
	}
	innerMap[field] = struct{}{}

	return nil
}

func (dr *BSONDriver) RemoveField(label string, field string) error {
	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	if fieldsForLabel, ok := dr.Fields[label]; ok {
		delete(fieldsForLabel, field)
		if len(fieldsForLabel) == 0 {
			delete(dr.Fields, label)
		}
	}

	key := benchtop.FieldLabelKey(field, label)
	upperBound, err := calculate_upper_bound(key)
	if err != nil {
		return err
	}

	log.Infof("Deleting keys in range: [%q, %q)", key, upperBound)
	// Perform deletion in a bulk write transaction
	err = dr.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		return tx.DeleteRange(key, upperBound, &pebble.WriteOptions{Sync: true})
	})
	if err != nil {
		return fmt.Errorf("delete range failed: %w", err)
	}
	return nil
}

func calculate_upper_bound(key []byte) ([]byte, error){
	uBound := make([]byte, len(key))
	copy(uBound, key)
	for i := len(uBound) - 1; i >= 0; i-- {
	    uBound[i]++
		if uBound[i] != 0 {
			return uBound, nil
		}
	}
	// This should never be reached since we're using prefixes that don't start with 0xFF
	return nil, fmt.Errorf("failed to calculate upper bound")
}

func (dr *BSONDriver) LoadFields() {
	fPrefix := benchtop.FieldPrefix
	dr.Lock.Lock()
	defer dr.Lock.Unlock()
	dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(fPrefix); it.Valid() && bytes.HasPrefix(it.Key(), fPrefix); it.Next() {
			field, label, _, _ := benchtop.FieldKeyParse(it.Key())
			dr.Fields[label] = make(map[string]struct{})
			dr.Fields[label][field] = struct{}{}
		}
		log.Debugf("Loaded %d label-fields from Indices", len(dr.Fields))
		return nil
	})
}

type FieldInfo struct {
	Label string
	Field string
}

func (dr *BSONDriver) ListFields() ([]FieldInfo) {
	seenFields := make(map[string]map[string]struct{})
	fPrefix := benchtop.FieldPrefix
	var out []FieldInfo
	err := dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(fPrefix); it.Valid() && bytes.HasPrefix(it.Key(), fPrefix); it.Next() {
			field, label, _, _ := benchtop.FieldKeyParse(it.Key())
			// Initialize inner map if label not seen
			if _, exists := seenFields[label]; !exists {
				seenFields[label] = make(map[string]struct{})
			}
			// Add field if not seen for this label
			if _, exists := seenFields[label][field]; !exists {
				out = append(out, FieldInfo{Label: label[2:], Field: field})
				seenFields[label][field] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		log.Errorln("bsontable ListFields: ", err)
	}
	return out
}

func (dr *BSONDriver) RowIdsByHas(fltField string, fltValue any, fltOp benchtop.OperatorType) chan string {
	dr.Lock.RLock()
	defer dr.Lock.RUnlock()

	prefix := bytes.Join([][]byte{
		benchtop.FieldPrefix,
		[]byte(fltField),
		}, benchtop.FieldSep)

	out := make(chan string, 100)
	go func() {
		defer close(out)
		err := dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				_, _, value, rowID := benchtop.FieldKeyParse(it.Key())
				if tableFilters.ApplyFilterCondition(
					value,
					benchtop.FieldFilter{
						Field: fltField, Value: fltValue, Operator: fltOp,
					},
				) {
					out <- string(rowID)
				}
			}
			return nil
		})
		if err != nil {
			log.Errorf("Error in View for field %s: %s", fltField, err)
		}
	}()
	return out
}

func (dr *BSONDriver) RowIdsByLabelFieldValue(fltLabel string, fltField string, fltValue any, fltOp benchtop.OperatorType) chan string {
	log.WithFields(log.Fields{"label": fltLabel, "field": fltField, "value": fltValue}).Info("Running RowIdsByLabelFieldValue")
	dr.Lock.RLock()
	defer dr.Lock.RUnlock()

	prefix := benchtop.FieldLabelKey(fltField, fltLabel)
	out := make(chan string, 100)
	go func() {
		defer close(out)
		err := dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
					_, _, value, rowID := benchtop.FieldKeyParse(it.Key())
				if tableFilters.ApplyFilterCondition(
					value,
					benchtop.FieldFilter{
						Field: fltField, Value: fltValue, Operator: fltOp,
					},
				) {
					out <- string(rowID)
				}
			}
			return nil
		})
		if err != nil {
			log.Errorf("Error in View for field %s: %s", fltField, err)
		}
		return
	}()
	return out
}

func (dr *BSONDriver) GetIDsForLabel(label string) chan string {
	dr.Lock.RLock()
	defer dr.Lock.RUnlock()

	out := make(chan string, 100)
	go func() {
		defer close(out)
		table, err := dr.Get(label)
		if err != nil {
			log.Errorf("GetIdsForLabel: %s on table: %s", err, label)
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

func calculateUpperBound(prefix []byte) []byte {
	// Returns the upper bound for a range query to include all keys starting with prefix.
	// Appends 0x00 to prefix to ensure all keys with prefix are less than the bound.
	upperBound := make([]byte, len(prefix)+1)
	copy(upperBound, prefix)
	upperBound[len(prefix)] = 0x00
	return upperBound
}
