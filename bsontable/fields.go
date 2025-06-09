package bsontable

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/bmeg/benchtop"

	"github.com/bmeg/benchtop/pebblebulk"
	tableFilters "github.com/bmeg/benchtop/bsontable/filters"
	"github.com/bmeg/grip/log"
)

func (dr *BSONDriver) AddField(label, field string) error {
	dr.Lock.RLock()
	defer dr.Lock.RUnlock()
	foundTable, ok := dr.Tables[label]
	log.Debugf("Table with label '%s' not found when adding grids Field", label)
	if !ok {
		// If the table doesn't yet exist, write the index Key stub.
		err := dr.db.Set(
			benchtop.FieldKey(label, field, nil, nil),
			[]byte{},
			nil,
		)
		if err != nil {
			return err
		}
	} else {
		rowChan, err := foundTable.Scan(true, nil)
		if err != nil {
			return err
		}
		log.Infoln("HELLO WE HERE", rowChan)

		err = dr.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
			for r := range rowChan {
				log.Infoln("R: ", r)
				err := tx.Set(benchtop.FieldKey(label, field, r[field], []byte(r["_key"].(string))),
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

func (dr *BSONDriver) RemoveField(label string, field string, value any, rowId []byte) error {
	dr.Lock.Lock()
	defer dr.Lock.Unlock()
	delete(dr.Fields[label], field)
	delete(dr.Fields, label)
	key := benchtop.FieldKey(label, field, value, rowId)
	err := dr.db.DeleteRange(
		key,
		calculateUpperBound(key),
		nil,
	)
	if err != nil {
		return err
	}
	return nil
}

func (dr *BSONDriver) LoadFields() {
	fPrefix := benchtop.FieldPrefix
	dr.Lock.Lock()
	defer dr.Lock.Unlock()
	dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(fPrefix); it.Valid() && bytes.HasPrefix(it.Key(), fPrefix); it.Next() {
			field, _, label, _ := benchtop.FieldKeyParse(it.Key())
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

func (dr *BSONDriver) ListFields() []FieldInfo {
	seenFields := make(map[string]map[string]struct{})
	fPrefix := benchtop.FieldPrefix
	var out []FieldInfo
	dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(fPrefix); it.Valid() && bytes.HasPrefix(it.Key(), fPrefix); it.Next() {
			field, _, label, _ := benchtop.FieldKeyParse(it.Key())
			if _, exists := seenFields[label]; !exists {
				seenFields[label] = make(map[string]struct{})
				if _, exists := seenFields[label][field]; !exists {
					// going to have a prefix attached to it "v_" or "e_" but user doesn't want to see this
					out = append(out, FieldInfo{Label: label[2:], Field: field})
					seenFields[label][field] = struct{}{}
				}
			}
		}
		return nil
	})
	return out
}

func (dr *BSONDriver) RowIdsByHas(fltField string, fltValue any, fltOp benchtop.OperatorType) chan string {
	dr.Lock.RLock()
	defer dr.Lock.RUnlock()

	//prefix := benchtop.FieldKey(fltField, "", nil, nil)
	prefix := bytes.Join([][]byte{
		benchtop.FieldPrefix,
		[]byte(fltField),
	}, benchtop.FieldSep)
	out := make(chan string, 100)
	go func() {
		defer close(out)
		err := dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				_, value, _, rowID := benchtop.FieldKeyParse(it.Key())
				if tableFilters.ApplyFilterCondition(
					value,
					benchtop.FieldFilter{
						Field: fltField, Value: fltValue, Operator: fltOp,
					},
				) {
					log.Debugln("Lookup - Found Key (hex):", fltField, fltValue, fltOp, value, rowID)
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

func (dr *BSONDriver) RowIdsByLabelFieldValue(fltLabel string, fltField string, fltValue any, fltOp benchtop.OperatorType) (chan string, error) {
	log.WithFields(log.Fields{"label": fltLabel, "field": fltField, "value": fltValue}).Info("Running RowIdsByLabelFieldValue")
	dr.Lock.RLock()
	defer dr.Lock.RUnlock()

	valueBytes, err := json.Marshal(fltValue)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal value: %v", err)
	}

	log.Debugln("LABEL: ", fltLabel, "FIELD: ", fltField, "VALUE: ", fltValue, "OP: ", fltOp)

	prefix := bytes.Join([][]byte{
		benchtop.FieldPrefix,
		[]byte(fltField),
		valueBytes,
		[]byte(fltLabel),
	}, benchtop.FieldSep)

	out := make(chan string, 100)
	go func() {
		defer close(out)
		dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				_, value, _, rowID := benchtop.FieldKeyParse(it.Key())
				if tableFilters.ApplyFilterCondition(
					value,
					benchtop.FieldFilter{
						Field: fltField, Value: fltValue, Operator: fltOp,
					},
				) {
					log.Debugln("Lookup - Found Key (hex):", fltField, fltValue, fltOp, value, rowID)
					out <- string(rowID)
				}
			}
			return nil
		})
		return
	}()
	return out, nil
}

func (dr *BSONDriver) GetIDsForLabel(label string) chan string {
	dr.Lock.RLock()
	defer dr.Lock.RUnlock()

	out := make(chan string, 100)
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

func calculateUpperBound(prefix []byte) []byte {
	// Finds the last possible key that starts with the prefix specified
	upperBound := make([]byte, len(prefix))
	copy(upperBound, prefix)
	for i := len(upperBound) - 1; i >= 0; i-- {
		upperBound[i]++
		if upperBound[i] != 0 {
			return upperBound
		}
	}
	allZeros := make([]byte, len(upperBound))
	if bytes.Equal(upperBound, allZeros) && len(upperBound) > 0 {
		return append(prefix, 0x00)
	}
	return upperBound
}
