package jsontable

import (
	"fmt"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/benchtop/jsontable/tpath"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/go-multierror"
)

func (dr *JSONDriver) BulkLoad(id uint16, rows chan *benchtop.Row) error {
	return dr.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		return dr.BulkLoadInternal(id, rows, tx)
	})
}

func (dr *JSONDriver) BulkLoadInternal(targetID uint16, inputs chan *benchtop.Row, tx *pebblebulk.PebbleBulk) error {
	if dr.Pkv == nil || dr.Pkv.Db == nil {
		return fmt.Errorf("pebble database instance is nil")
	}
	if tx == nil {
		return fmt.Errorf("passed pebble bulk transaction is nil")
	}

	const batchSize = 1000
	batch := make([]*benchtop.Row, 0, batchSize)

	for row := range inputs {
		if row == nil {
			continue
		}
		batch = append(batch, row)
		if len(batch) >= batchSize {
			if err := dr.processBatch(tx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	return dr.processBatch(tx, batch)
}

func (dr *JSONDriver) processBatch(tx *pebblebulk.PebbleBulk, entries []*benchtop.Row) error {
	if len(entries) == 0 {
		return nil
	}

	// Group rows by TableID
	byTable := make(map[uint16][]*benchtop.Row)
	for _, row := range entries {
		byTable[row.TableID] = append(byTable[row.TableID], row)
	}

	var errs *multierror.Error

	for tid, rows := range byTable {
		dr.Lock.RLock()
		tbl, ok := dr.Tables[tid]
		dr.Lock.RUnlock()

		if !ok {
			t, err := dr.Get(tid)
			if err != nil {
				errs = multierror.Append(errs, fmt.Errorf("BulkLoad: table ID %d not found: %v", tid, err))
				continue
			}
			tbl = t.(*table.JSONTable)
		}

		// uniqueRows will hold only rows that don't exist in the DB or this batch
		uniqueRows := make([]benchtop.Row, 0, len(rows))

		for _, r := range rows {
			// Persistent Existence Check
			// Uses NewPosKey (P | TableID | rowID) to check the Primary Index
			// tx.Get is now batch-aware, so it sees both the DB and previous writes in this session.
			pKey := benchtop.NewPosKey(tid, r.Id)
			_, closer, err := tx.Get(pKey)
			if err == nil {
				closer.Close()
				continue // Row already exists, skip
			}

			// If the error is anything other than NotFound, we have a DB issue
			if err != pebble.ErrNotFound {
				errs = multierror.Append(errs, err)
				continue
			}

			uniqueRows = append(uniqueRows, *r)
		}

		// If all rows in this batch were duplicates, skip to next table
		if len(uniqueRows) == 0 {
			continue
		}

		// 4. Bulk add ONLY the truly unique rows to the physical storage
		locs, err := tbl.AddRows(uniqueRows)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		// 5. Update Pebble Indices and Metadata
		for i, row := range uniqueRows {
			rowLoc := locs[i]

			// Primary Index: Maps RowID to Section/Offset
			if err := dr.AddTableEntryInfo(tx, row.Id, rowLoc); err != nil {
				errs = multierror.Append(errs, err)
			}

			// Secondary Indices (Field Index and Reverse Field Index)
			for field := range tbl.Fields {
				if val := tpath.PathLookup(row.Data, field); val != nil {
					// F | field | value | tableID | rowID
					fKey := benchtop.FieldKey(field, tid, val, row.Id)
					if err := tx.Set(fKey, []byte{}, nil); err != nil {
						errs = multierror.Append(errs, err)
					}

					// R | TableID | field | rowId
					rKey := benchtop.RFieldKey(tid, field, string(row.Id))
					bVal, err := sonic.ConfigFastest.Marshal(val)
					if err == nil {
						if err := tx.Set(rKey, bVal, nil); err != nil {
							errs = multierror.Append(errs, err)
						}
					}
				}
			}
		}
	}

	return errs.ErrorOrNil()
}
