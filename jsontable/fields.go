package jsontable

import (
	"bytes"
	"fmt"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"

	"github.com/bmeg/benchtop/filters"
	jTable "github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/benchtop/jsontable/tpath"
	"github.com/bmeg/benchtop/util"

	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/gripql"
)

func (dr *JSONDriver) AddField(label, field string) error {
	dr.Lock.Lock()

	table, ok := dr.Tables[label]
	if !ok {
		dr.Lock.Unlock()
		newTable, err := dr.New(label, nil)
		if err != nil {
			return err
		}
		table = newTable.(*jTable.JSONTable)

		dr.Lock.Lock()
		log.Debugf("Creating index '%s' for table '%s' that has not been written yet", field, label)
		// If the table doesn't yet exist, write the index Key stub.
		err = dr.Pkv.Set(
			benchtop.FieldKey(field, label, nil, nil),
			[]byte{},
			nil,
		)
		if err != nil {
			dr.Lock.Unlock()
			log.Errorf("Err attempting to add field %v", err)
			return err
		}
		err = dr.Pkv.Set(
			bytes.Join([][]byte{
				benchtop.RFieldPrefix,
				[]byte(label),
				[]byte(field),
			}, benchtop.FieldSep),
			[]byte{},
			nil,
		)
		if err != nil {
			dr.Lock.Unlock()
			log.Errorf("Err attempting to add field %v", err)
			return err
		}
	} else {
		log.Debugf("Found table %s writing indices for field %s", label, field)
		// Release lock while scanning to avoid blocking other operations
		dr.Lock.Unlock()
		err := dr.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
			var filter benchtop.RowFilter = nil
			for r := range table.ScanFull(filter) {
				fieldValue := tpath.PathLookup(r.DataMap, field)
				rowId, ok := r.DataMap["_id"].(string)
				if !ok {
					return fmt.Errorf("_id field not found or is not string in map %s", r.DataMap)
				}
				err := tx.Set(
					benchtop.FieldKey(
						field,
						label,
						fieldValue,
						[]byte(rowId),
					),
					benchtop.EncodeRowLoc(r.Loc),
					nil,
				)
				if err != nil {
					return err
				}
				if fieldValue != nil {
					byteFV, err := sonic.ConfigFastest.Marshal(fieldValue)
					if err != nil {
						return err
					}
					err = tx.Set(benchtop.RFieldKey(label, field, rowId), byteFV, nil)
					if err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		dr.Lock.Lock()
	}

	if table.Fields == nil {
		table.Fields = map[string]struct{}{}
	}
	if _, existsField := table.Fields[field]; existsField {
		dr.Lock.Unlock()
		return fmt.Errorf("index label '%s' field '%s' already exists", label, field)
	}
	table.Fields[field] = struct{}{}
	dr.Lock.Unlock()
	log.Debugln("List Fields: ", table.Fields)

	return nil
}

func (dr *JSONDriver) RemoveField(label string, field string) error {
	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	if table, ok := dr.Tables[label]; ok {
		delete(table.Fields, field)
	}
	FieldPrefix := benchtop.FieldLabelKey(field, label)
	RFieldKeyPrefix := bytes.Join([][]byte{
		benchtop.RFieldPrefix,
		[]byte(label),
		[]byte(field),
	}, benchtop.FieldSep)

	// Perform deletion in a bulk write transaction
	err := dr.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		if err := tx.DeletePrefix(FieldPrefix); err != nil {
			return fmt.Errorf("delete field prefix failed: %w", err)
		}
		if err := tx.DeletePrefix(RFieldKeyPrefix); err != nil {
			return fmt.Errorf("delete row index prefix failed: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (dr *JSONDriver) LoadFields() error {
	/*
	 * Not sure wether to use a cache here as well or keep it how it is.
	 */
	fPrefix := benchtop.FieldPrefix
	count := 0
	err := dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(fPrefix); it.Valid() && bytes.HasPrefix(it.Key(), fPrefix); it.Next() {
			field, label, _, _ := benchtop.FieldKeyParse(it.Key())

			dr.Lock.RLock()
			table, exists := dr.Tables[label]
			dr.Lock.RUnlock()

			if !exists {
				var err error
				tableStore, err := dr.New(label, nil)
				if err != nil {
					return err
				}
				table = tableStore.(*jTable.JSONTable)
			}

			dr.Lock.Lock()
			if table.Fields == nil {
				table.Fields = make(map[string]struct{})
			}
			if _, exists := table.Fields[field]; !exists {
				table.Fields[field] = struct{}{}
				count++
			}
			dr.Lock.Unlock()
		}
		log.Debugf("Loaded %d indices", count)
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

func (dr *JSONDriver) ListFields() []FieldInfo {
	/* Lists loaded fields.
	 * Since fields on disk are loaded on startup this should be all that is needed */

	dr.Lock.RLock()
	defer dr.Lock.RUnlock()

	var out []FieldInfo
	for _, table := range dr.Tables {
		if table.Fields != nil {
			for fieldName, _ := range table.Fields {
				if table.Name[:2] == "v_" {
					out = append(out, FieldInfo{Label: table.Name[2:], Field: fieldName})
				} else {
					out = append(out, FieldInfo{Label: table.Name, Field: fieldName})
				}

			}
		}
	}
	return out
}

func (dr *JSONDriver) DeleteRowField(label, field, rowID string) error {
	/* Deletes a singular row index field */
	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	// Check if the table exists
	_, ok := dr.Tables[label]
	if !ok {
		_, err := dr.New(label, nil)
		if err != nil {
			return err
		}

	}

	if len(dr.Tables[label].Fields) <= 0 {
		log.Errorf("No fields defined for table '%s'", label)
		return fmt.Errorf("no fields defined for table '%s'", label)
	}

	if _, existsField := dr.Tables[label].Fields[field]; !existsField {
		log.Errorf("Field '%s' does not exist in table '%s'", field, label)
		return fmt.Errorf("field '%s' does not exist in table '%s'", field, label)
	}

	// Get the field value from the reverse index
	rowIndexKey := benchtop.RFieldKey(label, field, rowID)
	var fieldValueBytes []byte
	err := dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		var err error
		if it.Seek(rowIndexKey); it.Valid() && bytes.Equal(it.Key(), rowIndexKey) {
			fieldValueBytes, err = it.Value()
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Errorf("Error finding reverse index for row '%s' in table '%s' for field '%s': %v", rowID, label, field, err)
		return err
	}

	// If no reverse index entry exists, no index to delete
	if fieldValueBytes == nil {
		log.Debugf("No index entry for row '%s' in table '%s' for field '%s'", rowID, label, field)
		return nil
	}

	var fieldValue any
	if err := sonic.ConfigFastest.Unmarshal(fieldValueBytes, &fieldValue); err != nil {
		log.Errorf("Error deserializing field value for row '%s' in table '%s' for field '%s': %v", rowID, label, field, err)
		return err
	}

	// Delete both the forward and reverse index entries
	err = dr.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		if err := tx.Delete(benchtop.FieldKey(field, label, fieldValue, []byte(rowID)), nil); err != nil {
			return err
		}
		if err := tx.Delete(rowIndexKey, nil); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Errorf("Error deleting index for field '%s' in table '%s' for row '%s': %v", field, label, rowID, err)
		return err
	}
	log.Debugf("Successfully deleted index for field '%s' in table '%s' for row '%s'", field, label, rowID)
	return nil
}

func (dr *JSONDriver) RowIdsByHas(fltField string, fltValue any, fltOp gripql.Condition) chan benchtop.Index {
	log.WithFields(log.Fields{"field": fltField, "value": fltValue, "op": fltOp}).Debug("Running RowIdsByHas")

	if fltOp == gripql.Condition_EQ || fltOp == gripql.Condition_WITHIN {
		out := make(chan benchtop.Index, 100)
		go func() {
			defer close(out)
			dr.Lock.RLock()
			var labels []string
			for label, table := range dr.Tables {
				if _, ok := table.Fields[fltField]; ok {
					labels = append(labels, label)
				}
			}
			dr.Lock.RUnlock()

			vals := []any{fltValue}
			if fltOp == gripql.Condition_WITHIN {
				vals = util.SliceToAny(fltValue)
			}

			dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
				for _, label := range labels {
					for _, v := range vals {
						prefix := benchtop.FieldKey(fltField, label, v, nil)
						for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
							parts := bytes.Split(it.Key(), benchtop.FieldSep)
							if len(parts) >= 5 {
								val, _ := it.Value()
								loc := benchtop.DecodeRowLoc(val)
								out <- benchtop.Index{Key: parts[4], Loc: loc}
							}
						}
					}
				}
				return nil
			})
		}()
		return out
	}

	dr.Lock.RLock()
	defer dr.Lock.RUnlock()

	prefix := bytes.Join([][]byte{
		benchtop.FieldPrefix,
		[]byte(fltField),
	}, benchtop.FieldSep)

	out := make(chan benchtop.Index, 100)
	go func() {
		defer close(out)
		err := dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				_, _, value, rowID := benchtop.FieldKeyParse(it.Key())
				if filters.ApplyFilterCondition(
					value,
					&filters.FieldFilter{
						Field: fltField, Value: fltValue, Operator: fltOp,
					},
				) {
					v, _ := it.Value()
					loc := benchtop.DecodeRowLoc(v)
					out <- benchtop.Index{Key: rowID, Loc: loc}
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

func (dr *JSONDriver) RowIdsByLabelFieldValue(fltLabel string, fltField string, fltValue any, fltOp gripql.Condition) chan benchtop.Index {
	log.WithFields(log.Fields{"label": fltLabel, "field": fltField, "value": fltValue, "op": fltOp}).Debug("Running RowIdsByLabelFieldValue")

	if fltOp == gripql.Condition_EQ {
		out := make(chan benchtop.Index, 100)
		go func() {
			defer close(out)
			prefix := benchtop.FieldKey(fltField, fltLabel, fltValue, nil)
			dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
				for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
					parts := bytes.Split(it.Key(), benchtop.FieldSep)
					if len(parts) >= 5 {
						val, _ := it.Value()
						loc := benchtop.DecodeRowLoc(val)
						out <- benchtop.Index{Key: parts[4], Loc: loc}
					}
				}
				return nil
			})
		}()
		return out
	}

	if fltOp == gripql.Condition_WITHIN {
		out := make(chan benchtop.Index, 100)
		go func() {
			defer close(out)
			vals := util.SliceToAny(fltValue)
			dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
				for _, v := range vals {
					prefix := benchtop.FieldKey(fltField, fltLabel, v, nil)
					for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
						parts := bytes.Split(it.Key(), benchtop.FieldSep)
						if len(parts) >= 5 {
							val, _ := it.Value()
							loc := benchtop.DecodeRowLoc(val)
							out <- benchtop.Index{Key: parts[4], Loc: loc}
						}
					}
				}
				return nil
			})
		}()
		return out
	}

	dr.Lock.RLock()
	defer dr.Lock.RUnlock()

	prefix := benchtop.FieldLabelKey(fltField, fltLabel)
	out := make(chan benchtop.Index, 100)
	go func() {
		defer close(out)
		err := dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				_, _, value, rowID := benchtop.FieldKeyParse(it.Key())
				if filters.ApplyFilterCondition(
					value,
					&filters.FieldFilter{
						Field: fltField, Value: fltValue, Operator: fltOp,
					},
				) {
					v, _ := it.Value()
					loc := benchtop.DecodeRowLoc(v)
					out <- benchtop.Index{Key: rowID, Loc: loc}
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

func (dr *JSONDriver) GetIDsForLabel(label string) chan string {
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
		for id := range table.ScanId(filter) {
			out <- id
		}
	}()
	return out
}
