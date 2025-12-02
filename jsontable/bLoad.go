package jsontable

import (
	"fmt"
	"sync"

	"github.com/bmeg/benchtop"
	jTable "github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bytedance/sonic"
	"github.com/hashicorp/go-multierror"
)

func (dr *JSONDriver) BulkLoad(inputs chan *benchtop.Row, tx *pebblebulk.PebbleBulk) error {
	if dr.Pkv == nil || dr.Pkv.Db == nil {
		return fmt.Errorf("pebble database instance is nil")
	}
	var wg sync.WaitGroup
	tableChannels := make(map[string]chan *benchtop.Row)

	metadataChan := make(chan *jTable.KitchenSink, 100)

	snapshot := dr.Pkv.Db.NewSnapshot()
	defer snapshot.Close()

	for row := range inputs {
		if _, exists := tableChannels[row.TableName]; !exists {
			dr.Lock.RLock()
			table, exists := dr.Tables[row.TableName]
			dr.Lock.RUnlock()
			if !exists {
				var localErr *multierror.Error
				newTable, err := dr.New(row.TableName, nil)
				if err != nil {
					localErr = multierror.Append(localErr, fmt.Errorf("failed to create table %s: %v", row.TableName, err))
					metadataChan <- &jTable.KitchenSink{
						FieldIndexKeyElements: nil,
						Metadata:              nil,
						Err:                   localErr.ErrorOrNil(),
					}
					continue
				}
				table = newTable.(*jTable.JSONTable)
				dr.Lock.Lock()
				dr.Tables[row.TableName] = table
				dr.Lock.Unlock()
			}
			inputChan := table.StartTableGoroutine(&wg, metadataChan, snapshot, BATCH_SIZE)
			tableChannels[row.TableName] = inputChan
		}
		tableChannels[row.TableName] <- row
	}
	for _, ch := range tableChannels {
		close(ch)
	}

	var errs *multierror.Error
	done := make(chan struct{})
	go func() {
		defer close(done)
		writeFunc := func(tx *pebblebulk.PebbleBulk) error {
			for meta := range metadataChan {
				if meta.Err != nil {
					errs = multierror.Append(errs, meta.Err)
					continue
				}
				if meta.Metadata == nil {
					continue
				}
				for _, keyElements := range meta.FieldIndexKeyElements {
					forwardKey := benchtop.FieldKey(keyElements.Field, keyElements.TableName, keyElements.Val, []byte(keyElements.RowId))
					if err := tx.Set(forwardKey, []byte{}, nil); err != nil {
						errs = multierror.Append(errs, err)
					}
					BVal, err := sonic.ConfigFastest.Marshal(keyElements.Val)
					if err != nil {
						errs = multierror.Append(errs, err)
						continue
					}
					if err := tx.Set(benchtop.RFieldKey(keyElements.TableName, keyElements.Field, keyElements.RowId), BVal, nil); err != nil {
						errs = multierror.Append(errs, err)
					}
				}

				// Write row location entries.
				for id, m := range meta.Metadata {
					dr.LocCache.Set(id, m)
					dr.AddTableEntryInfo(tx, []byte(id), m)
				}
			}
			return nil
		}

		if tx == nil {
			errs = multierror.Append(errs, fmt.Errorf("pebble bulk instance passed into BulkLoad function is nil"))
		} else {
			dr.PebbleLock.Lock()
			if err := writeFunc(tx); err != nil {
				errs = multierror.Append(errs, err)
			}
			dr.PebbleLock.Unlock()
		}
	}()

	wg.Wait()
	close(metadataChan)
	<-done

	return errs.ErrorOrNil()
}
