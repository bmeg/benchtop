package jsontable

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"

	"github.com/bmeg/benchtop/filters"
	"github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/benchtop/jsontable/tpath"
	"github.com/bmeg/benchtop/query"
	"github.com/bmeg/benchtop/util"

	"github.com/bmeg/benchtop/pebblebulk"
)

func (dr *JSONDriver) lookupPosLoc(label string, rowID []byte) *benchtop.RowLoc {
	dr.Lock.RLock()
	tbl, ok := dr.Tables[label]
	dr.Lock.RUnlock()
	if !ok || tbl == nil {
		return nil
	}
	val, closer, err := dr.Pkv.Get(benchtop.NewPosKey(tbl.TableId, rowID))
	if err != nil {
		if !errors.Is(err, pebble.ErrNotFound) {
			log.Errorf("lookupPosLoc(%s,%s): %v", label, string(rowID), err)
		}
		return nil
	}
	defer closer.Close()
	return benchtop.DecodeRowLoc(val)
}

func (dr *JSONDriver) AddField(label, field string) error {
	dr.Lock.Lock()

	tbl, ok := dr.Tables[label]
	if !ok {
		dr.Lock.Unlock()
		newTable, err := dr.New(label, nil)
		if err != nil {
			return err
		}
		tbl = newTable.(*table.JSONTable)

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
			for r := range tbl.ScanFull(filter) {
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

	if tbl.Fields == nil {
		tbl.Fields = map[string]struct{}{}
	}
	if _, existsField := tbl.Fields[field]; existsField {
		dr.Lock.Unlock()
		return fmt.Errorf("index label '%s' field '%s' already exists", label, field)
	}
	tbl.Fields[field] = struct{}{}
	dr.Lock.Unlock()
	log.Debugln("List Fields: ", tbl.Fields)

	return nil
}

func (dr *JSONDriver) RemoveField(label string, field string) error {
	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	if tbl, ok := dr.Tables[label]; ok {
		delete(tbl.Fields, field)
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
			tbl, exists := dr.Tables[label]
			dr.Lock.RUnlock()

			if !exists {
				var err error
				tblStore, err := dr.New(label, nil)
				if err != nil {
					return err
				}
				tbl = tblStore.(*table.JSONTable)
			}

			dr.Lock.Lock()
			if tbl.Fields == nil {
				tbl.Fields = make(map[string]struct{})
			}
			if _, exists := tbl.Fields[field]; !exists {
				tbl.Fields[field] = struct{}{}
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

func (dr *JSONDriver) GetAllColNames() chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		dr.Lock.RLock()
		defer dr.Lock.RUnlock()
		seen := make(map[string]struct{})
		for _, tbl := range dr.Tables {
			for field := range tbl.Fields {
				if _, ok := seen[field]; !ok {
					out <- field
					seen[field] = struct{}{}
				}
			}
		}
	}()
	return out
}

func (dr *JSONDriver) GetLabels(edges bool, removePrefix bool) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		dr.Lock.RLock()
		defer dr.Lock.RUnlock()
		for label := range dr.Tables {
			isEdge := strings.HasPrefix(label, "e_")
			if (edges && isEdge) || (!edges && !isEdge) {
				if removePrefix && len(label) > 2 {
					out <- label[2:]
				} else {
					out <- label
				}
			}
		}
	}()
	return out
}

func (dr *JSONDriver) ListFields() []benchtop.FieldInfo {
	/* Lists loaded fields.
	 * Since fields on disk are loaded on startup this should be all that is needed */

	dr.Lock.RLock()
	defer dr.Lock.RUnlock()

	var out []benchtop.FieldInfo
	for _, tbl := range dr.Tables {
		if tbl.Fields != nil {
			for fieldName, _ := range tbl.Fields {
				if len(tbl.Name) > 2 && tbl.Name[:2] == "v_" {
					out = append(out, benchtop.FieldInfo{Label: tbl.Name[2:], Field: fieldName})
				} else {
					out = append(out, benchtop.FieldInfo{Label: tbl.Name, Field: fieldName})
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

func (dr *JSONDriver) RowIdsByHas(fltField string, fltValue any, fltOp query.Condition) chan benchtop.Index {
	log.WithFields(log.Fields{"field": fltField, "value": fltValue, "op": fltOp}).Debug("Running RowIdsByHas")

	if fltOp == query.EQ || fltOp == query.WITHIN {
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
			if len(labels) == 0 {
				for idx := range dr.scanRowsByField("", fltField, fltValue, fltOp) {
					out <- idx
				}
				return
			}

			vals := []any{fltValue}
			if fltOp == query.WITHIN {
				vals = util.SliceToAny(fltValue)
			}

			dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
				for _, label := range labels {
					for _, v := range vals {
							prefix := benchtop.FieldKey(fltField, label, v, nil)
							for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
								parts := bytes.Split(it.Key(), benchtop.FieldSep)
								if len(parts) >= 5 {
									loc := dr.lookupPosLoc(label, parts[4])
									if loc == nil {
										continue
									}
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
		hasIndexedLabel := false
		dr.Lock.RLock()
		for _, table := range dr.Tables {
			if _, ok := table.Fields[fltField]; ok {
				hasIndexedLabel = true
				break
			}
		}
		dr.Lock.RUnlock()
		if !hasIndexedLabel {
			for idx := range dr.scanRowsByField("", fltField, fltValue, fltOp) {
				out <- idx
			}
			return
		}
		err := dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				_, _, value, rowID := benchtop.FieldKeyParse(it.Key())
					if filters.ApplyFilterCondition(
						value,
						&filters.FieldFilter{
							Field: fltField, Value: fltValue, Operator: fltOp,
						},
					) {
						parts := bytes.Split(it.Key(), benchtop.FieldSep)
						if len(parts) < 5 {
							continue
						}
						loc := dr.lookupPosLoc(string(parts[2]), rowID)
						if loc == nil {
							continue
						}
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

func (dr *JSONDriver) RowIdsByLabelFieldValue(fltLabel string, fltField string, fltValue any, fltOp query.Condition) chan benchtop.Index {
	log.WithFields(log.Fields{"label": fltLabel, "field": fltField, "value": fltValue, "op": fltOp}).Debug("Running RowIdsByLabelFieldValue")

	if fltOp == query.EQ {
		out := make(chan benchtop.Index, 100)
		go func() {
			defer close(out)
			dr.Lock.RLock()
			tbl, ok := dr.Tables[fltLabel]
			dr.Lock.RUnlock()
			if !ok || tbl.Fields == nil {
				for idx := range dr.scanRowsByField(fltLabel, fltField, fltValue, fltOp) {
					out <- idx
				}
				return
			}
			if _, ok := tbl.Fields[fltField]; !ok {
				for idx := range dr.scanRowsByField(fltLabel, fltField, fltValue, fltOp) {
					out <- idx
				}
				return
			}
				prefix := benchtop.FieldKey(fltField, fltLabel, fltValue, nil)
				dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
					for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
						parts := bytes.Split(it.Key(), benchtop.FieldSep)
						if len(parts) >= 5 {
							loc := dr.lookupPosLoc(fltLabel, parts[4])
							if loc == nil {
								continue
							}
							out <- benchtop.Index{Key: parts[4], Loc: loc}
						}
					}
					return nil
			})
		}()
		return out
	}

	if fltOp == query.WITHIN {
		out := make(chan benchtop.Index, 100)
		go func() {
			defer close(out)
			dr.Lock.RLock()
			tbl, ok := dr.Tables[fltLabel]
			dr.Lock.RUnlock()
			if !ok || tbl.Fields == nil {
				for idx := range dr.scanRowsByField(fltLabel, fltField, fltValue, fltOp) {
					out <- idx
				}
				return
			}
			if _, ok := tbl.Fields[fltField]; !ok {
				for idx := range dr.scanRowsByField(fltLabel, fltField, fltValue, fltOp) {
					out <- idx
				}
				return
			}
			vals := util.SliceToAny(fltValue)
			dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
				for _, v := range vals {
					prefix := benchtop.FieldKey(fltField, fltLabel, v, nil)
					for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
						parts := bytes.Split(it.Key(), benchtop.FieldSep)
						if len(parts) >= 5 {
							loc := dr.lookupPosLoc(fltLabel, parts[4])
							if loc == nil {
								continue
							}
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
		dr.Lock.RLock()
		tbl, ok := dr.Tables[fltLabel]
		dr.Lock.RUnlock()
		if !ok || tbl.Fields == nil {
			for idx := range dr.scanRowsByField(fltLabel, fltField, fltValue, fltOp) {
				out <- idx
			}
			return
		}
		if _, ok := tbl.Fields[fltField]; !ok {
			for idx := range dr.scanRowsByField(fltLabel, fltField, fltValue, fltOp) {
				out <- idx
			}
			return
		}
		err := dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				_, _, value, rowID := benchtop.FieldKeyParse(it.Key())
					if filters.ApplyFilterCondition(
						value,
						&filters.FieldFilter{
							Field: fltField, Value: fltValue, Operator: fltOp,
						},
					) {
						loc := dr.lookupPosLoc(fltLabel, rowID)
						if loc == nil {
							continue
						}
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

func (dr *JSONDriver) scanRowsByField(label, field string, value any, op query.Condition) chan benchtop.Index {
	out := make(chan benchtop.Index, 100)
	go func() {
		defer close(out)

		dr.Lock.RLock()
		targetTables := make([]*table.JSONTable, 0, len(dr.Tables))
		if label != "" {
			if tbl, ok := dr.Tables[label]; ok {
				targetTables = append(targetTables, tbl)
			}
		} else {
			for _, tbl := range dr.Tables {
				targetTables = append(targetTables, tbl)
			}
		}
		dr.Lock.RUnlock()

		cond := &filters.FieldFilter{Field: field, Value: value, Operator: op}
		for _, tbl := range targetTables {
			for row := range tbl.ScanFull(nil) {
				fieldVal := tpath.PathLookup(row.DataMap, field)
				if !filters.ApplyFilterCondition(fieldVal, cond) {
					continue
				}
				rowID, ok := row.DataMap["_id"].(string)
				if !ok || rowID == "" {
					continue
				}
				out <- benchtop.Index{Key: []byte(rowID), Loc: row.Loc}
			}
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

		tbl, err := dr.Get(label)
		if err != nil {
			log.Errorf("GetIdsForLabel: %s on table: %s", err, label)
			return
		}

		var filter benchtop.RowFilter = nil
		for id := range tbl.ScanId(filter) {
			out <- id
		}
	}()
	return out
}
