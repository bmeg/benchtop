package bsontable

import (
	"bytes"
	"fmt"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/grip/log"

	tableFilters "github.com/bmeg/benchtop/bsontable/filters"
	"github.com/bmeg/benchtop/pebblebulk"
)

func (dr *BSONDriver) AddField(label, field string) error {
	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	foundTable, ok := dr.Tables[label]
	if !ok {
		log.Debugf("Creating index '%s' for table '%s' that has not been written yet", field, label)
		// If the table doesn't yet exist, write the index Key stub.
		err := dr.db.Set(
			benchtop.FieldKey(field, label, nil, nil),
			[]byte{},
			nil,
		)
		if err != nil {
			log.Errorf("Err attempting to add field %v", err)
			return err
		}
	} else {
		log.Debugf("Found table %s writing indices for field %s", label, field)
		err := dr.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
			var filter benchtop.RowFilter = nil
			for r := range foundTable.Scan(false, filter) {
				err := tx.Set(
					benchtop.FieldKey(
						field,
						label,
						PathLookup(
							r.(map[string]any), field),
						[]byte(r.(map[string]any)["_id"].(string),
						),
					),
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
	log.Debugln("Fields: ", dr.Fields)

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

	log.Infof("Deleting prefix: %q", key)
	// Perform deletion in a bulk write transaction
	err := dr.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		return tx.DeletePrefix(key)
	})
	if err != nil {
		return fmt.Errorf("delete range failed: %w", err)
	}
	return nil
}

func (dr *BSONDriver) LoadFields() error {
	/* 
	 * Not sure wether to use a cache here as well or keep it how it is.
	 */
	fPrefix := benchtop.FieldPrefix
	dr.Lock.Lock()
	defer dr.Lock.Unlock()
	err := dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(fPrefix); it.Valid() && bytes.HasPrefix(it.Key(), fPrefix); it.Next() {
			field, label, _, _ := benchtop.FieldKeyParse(it.Key())
			if _, exists := dr.Fields[label]; !exists {
				dr.Fields[label] = make(map[string]struct{})
			}
			if _, exists := dr.Fields[label][field]; !exists {
				dr.Fields[label][field] = struct{}{}
			}
		}
		log.Infof("Loaded %d label-fields from Indices", len(dr.Fields))
		return nil
	})
	if err != nil {
		log.Errorf("Err loading fields: %v", err)
		return err
	}
	return nil
}

type FieldInfo struct {
	Label string
	Field string
}

func (dr *BSONDriver) ListFields() []FieldInfo {
	/* Lists loaded fields.
	 * Since fields on disk are loaded on startup this should be all that is needed */

	dr.Lock.RLock()
	defer dr.Lock.RUnlock()

	var out []FieldInfo
	for label, fieldsMap := range dr.Fields {
		for fieldName := range fieldsMap {
			if label[:2] == "v_" {
				out = append(out, FieldInfo{Label: label[2:], Field: fieldName})
			} else {
				out = append(out, FieldInfo{Label: label, Field: fieldName})
			}
		}
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
					&benchtop.FieldFilter{
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
	log.WithFields(log.Fields{"label": fltLabel, "field": fltField, "value": fltValue}).Debug("Running RowIdsByLabelFieldValue")
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
					&benchtop.FieldFilter{
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

		var filter benchtop.RowFilter = nil
		for id := range table.Scan(true, filter) {
			out <- id.(string)
		}
	}()
	return out
}
