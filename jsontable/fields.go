package jsontable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"

	"github.com/bmeg/benchtop/filters"
	"github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/benchtop/jsontable/tpath"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/benchtop/query"
)

func (dr *JSONDriver) lookupPosLoc(tableID uint16, rowID []byte) *benchtop.RowLoc {
	dr.Lock.RLock()
	tbl, ok := dr.Tables[tableID]
	dr.Lock.RUnlock()
	if !ok || tbl == nil {
		return nil
	}
	val, closer, err := dr.Pkv.Get(benchtop.NewPosKey(tbl.TableId, rowID))
	if err != nil {
		if !errors.Is(err, pebble.ErrNotFound) {
			log.Errorf("lookupPosLoc(%s,%s): %v", tbl.Name, string(rowID), err)
		}
		return nil
	}
	defer closer.Close()
	loc := benchtop.DecodeRowLoc(val)
	if loc == nil {
		return nil
	}
	return loc
}

func (dr *JSONDriver) AddField(tableID uint16, field string) error {
	dr.Lock.Lock()

	tbl, ok := dr.Tables[tableID]
	if !ok {
		dr.Lock.Unlock()
		return fmt.Errorf("table ID %d not found", tableID)
	}
	label := tbl.Name

	log.Debugf("Creating index '%s' for table '%s' that has not been written yet", field, label)
	// If the table doesn't yet exist, write the index Key stub.
	err := dr.Pkv.Set(
		benchtop.FieldKey(field, tableID, nil, nil),
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
			binary.LittleEndian.AppendUint16(nil, tableID),
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

	log.Infof("Found table %s (ID %d); clearing and rebuilding indices for field %s", label, tbl.TableId, field)
	// Clean existing index for this field/label first to avoid stale entries
	// Use internal non-locking methods to avoid deadlock with AddField's own lock
	if err := dr.removeFieldIndexes(tableID, field); err != nil {
		log.Errorf("Failed to clear index for ID %d:%s before rebuild: %v", tableID, field, err)
	} else {
		log.Debugf("Successfully cleared stale indices for %s:%s", label, field)
	}
	// Release lock while scanning to avoid blocking other operations
	dr.Lock.Unlock()
	// Table exists, perform rebuild
	errRebuild := dr.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
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
					tableID,
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
				err = tx.Set(benchtop.RFieldKey(tableID, field, rowId), byteFV, nil)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	if errRebuild != nil {
		return errRebuild
	}
	dr.Lock.Lock()

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

func (dr *JSONDriver) RemoveField(tableID uint16, field string) error {
	dr.Lock.Lock()
	defer dr.Lock.Unlock()
	return dr.removeFieldLocked(tableID, field)
}

func (dr *JSONDriver) removeFieldLocked(tableID uint16, field string) error {
	tbl, ok := dr.Tables[tableID]
	if ok {
		delete(tbl.Fields, field)
	} else {
		return fmt.Errorf("table ID %d not found", tableID)
	}
	return dr.removeFieldIndexes(tableID, field)
}

func (dr *JSONDriver) removeFieldIndexes(tableID uint16, field string) error {
	FieldPrefix := benchtop.FieldLabelKey(field, tableID)

	RFieldKeyPrefix := bytes.Join([][]byte{
		benchtop.RFieldPrefix,
		binary.LittleEndian.AppendUint16(nil, tableID),
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
	return err
}

func (dr *JSONDriver) LoadFields() error {
	/*
	 * Not sure wether to use a cache here as well or keep it how it is.
	 */
	fPrefix := benchtop.FieldPrefix
	count := 0
	err := dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(fPrefix); it.Valid() && bytes.HasPrefix(it.Key(), fPrefix); it.Next() {
			field, tableID, _, _ := benchtop.FieldKeyParse(it.Key())
			if field == "" {
				log.Warnf("LoadFields: skipping malformed field key: %x", it.Key())
				continue
			}

			dr.Lock.RLock()
			tbl, exists := dr.Tables[tableID]
			dr.Lock.RUnlock()

			if !exists {
				// Attempt to load by ID
				tblStore, err := dr.Get(tableID)
				if err != nil {
					log.Errorf("LoadFields: could not load table ID %d: %v", tableID, err)
					continue
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

func (dr *JSONDriver) ListFields() []benchtop.FieldInfo {
	/* Lists loaded fields.
	 * Since fields on disk are loaded on startup this should be all that is needed */

	dr.Lock.RLock()
	defer dr.Lock.RUnlock()

	var out []benchtop.FieldInfo
	for _, tbl := range dr.Tables {
		if tbl.Fields != nil {
			for fieldName := range tbl.Fields {
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

func (dr *JSONDriver) DeleteRowField(tableID uint16, field, rowID string) error {
	/* Deletes a singular row index field */
	dr.Lock.Lock()
	defer dr.Lock.Unlock()

	// Check if the table exists
	tbl, ok := dr.Tables[tableID]
	if !ok {
		return fmt.Errorf("table ID %d not found", tableID)
	}
	label := tbl.Name

	if len(tbl.Fields) <= 0 {
		log.Errorf("No fields defined for table ID %d (%s)", tableID, label)
		return fmt.Errorf("no fields defined for table '%s'", label)
	}

	if _, existsField := tbl.Fields[field]; !existsField {
		log.Errorf("Field '%s' does not exist in table ID %d (%s)", field, tableID, label)
		return fmt.Errorf("field '%s' does not exist in table '%s'", field, label)
	}

	// Get the field value from the reverse index
	rowIndexKey := benchtop.RFieldKey(tableID, field, rowID)
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
		if err := tx.Delete(benchtop.FieldKey(field, tableID, fieldValue, []byte(rowID)), nil); err != nil {
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
	// Uses indices for EQ queries if Available, otherwise falls back to scanRowsByField.
	return dr.scanRowsByField(0, fltField, fltValue, fltOp)
}

func (dr *JSONDriver) RowIdsByTableFieldValue(tableID uint16, fltField string, fltValue any, fltOp query.Condition) chan benchtop.Index {
	log.WithFields(log.Fields{"tableID": tableID, "field": fltField, "value": fltValue, "op": fltOp}).Debug("Running RowIdsByTableFieldValue")

	// Uses fast index lookup for EQ if available, otherwise scans the table.
	return dr.scanRowsByField(tableID, fltField, fltValue, fltOp)
}

func (dr *JSONDriver) scanRowsByField(tableID uint16, field string, value any, op query.Condition) chan benchtop.Index {
	out := make(chan benchtop.Index, 100)
	go func() {
		defer close(out)

		dr.Lock.RLock()
		var targetTables []*table.JSONTable
		if tableID != 0 {
			if tbl, ok := dr.Tables[tableID]; ok {
				targetTables = append(targetTables, tbl)
			}
		} else {
			for _, tbl := range dr.Tables {
				targetTables = append(targetTables, tbl)
			}
		}
		dr.Lock.RUnlock()

		if len(targetTables) == 0 {
			return
		}

		// FAST PATH: If operator is EQ, use Pebble.
		if op == query.EQ {
			// For specific tables, check if they are indexed
			allIndexed := true
			for _, tbl := range targetTables {
				if len(tbl.Fields) == 0 {
					allIndexed = false
					break
				}
				if _, ok := tbl.Fields[field]; !ok {
					allIndexed = false
					break
				}
			}

			if allIndexed {
				if tableID == 0 {
					dr.scanGlobalIndex(field, value, out)
				} else {
					dr.scanTableIndex(targetTables, field, value, out)
				}
				return
			}
		}

		// SLOW PATH: Sequential scan.
		dr.scanSlow(targetTables, field, value, op, out)
	}()
	return out
}

func (dr *JSONDriver) scanGlobalIndex(field string, value any, out chan<- benchtop.Index) {
	prefix := benchtop.FieldValueKey(field, value)
	if prefix == nil {
		return
	}
	prefix = append(prefix, benchtop.FieldSep...)

	_ = dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		count := 0
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			val, err := it.Value()
			if err != nil {
				continue
			}
			loc := benchtop.DecodeRowLoc(val)
			if loc == nil {
				continue
			}
			_, _, _, rowID := benchtop.FieldKeyParse(it.Key())
			safeID := make([]byte, len(rowID))
			copy(safeID, rowID)
			out <- benchtop.Index{Key: safeID, Loc: loc}
			count++
			if count%1000 == 0 {
				log.Debugf("scanGlobalIndex: processed %d items", count)
			}
		}
		return nil
	})
}

func (dr *JSONDriver) scanTableIndex(targetTables []*table.JSONTable, field string, value any, out chan<- benchtop.Index) {
	_ = dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for _, tbl := range targetTables {
			prefix := benchtop.FieldValueKey(field, value)
			prefix = append(prefix, benchtop.FieldSep...)
			idBytes := make([]byte, 2)
			binary.LittleEndian.PutUint16(idBytes, tbl.TableId)
			prefix = append(prefix, idBytes...)
			prefix = append(prefix, benchtop.FieldSep...)

			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				val, err := it.Value()
				if err != nil {
					continue
				}
				loc := benchtop.DecodeRowLoc(val)
				if loc == nil {
					continue
				}
				_, _, _, rowID := benchtop.FieldKeyParse(it.Key())
				safeID := make([]byte, len(rowID))
				copy(safeID, rowID)
				out <- benchtop.Index{Key: safeID, Loc: loc}
			}
		}
		return nil
	})
}

func (dr *JSONDriver) scanSlow(targetTables []*table.JSONTable, field string, value any, op query.Condition, out chan<- benchtop.Index) {
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
}

func (dr *JSONDriver) GetIDsForTable(tableID uint16) chan string {
	dr.Lock.RLock()
	defer dr.Lock.RUnlock()

	out := make(chan string, 100)
	go func() {
		defer close(out)

		tbl, err := dr.Get(tableID)
		if err != nil {
			log.Errorf("GetIdsForTable: %s on ID: %d", err, tableID)
			return
		}

		var filter benchtop.RowFilter = nil
		for id := range tbl.ScanId(filter) {
			out <- id
		}
	}()
	return out
}
