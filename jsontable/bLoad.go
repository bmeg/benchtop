package jsontable

import (
	"fmt"
	"sync"

	"github.com/bmeg/benchtop"
	jTable "github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
	"github.com/hashicorp/go-multierror"
)

func (dr *JSONDriver) BulkLoad(inputs chan *benchtop.Row, tx *pebblebulk.PebbleBulk) error {
	if dr.Pkv == nil || dr.Pkv.Db == nil {
		return fmt.Errorf("pebble database instance is nil")
	}
	if tx == nil {
		return fmt.Errorf("passed pebble bulk transaction is nil")
	}

	var wg sync.WaitGroup
	tableChans := make(map[string]chan *benchtop.Row)
	metadataChan := make(chan *jTable.IngestBatch, 1024)

	// 1. Dispatcher: Route rows to table-specific goroutines
	wg.Add(1)
	go func() {
		defer wg.Done()
		for row := range inputs {
			ch, exists := tableChans[row.TableName]
			if !exists {
				dr.Lock.RLock()
				table, ok := dr.Tables[row.TableName]
				dr.Lock.RUnlock()

				if !ok {
					t, err := dr.New(row.TableName, nil)
					if err != nil {
						log.Errorf("BulkLoad: failed to auto-create table %s: %v", row.TableName, err)
						continue
					}
					table = t.(*jTable.JSONTable)
				}
				ch = table.StartTableGoroutine(&wg, metadataChan, BATCH_SIZE)
				tableChans[row.TableName] = ch
			}
			ch <- row
		}
		for _, ch := range tableChans {
			close(ch)
		}
	}()

	// 2. Writer: Process metadata and commit to Pebble
	var writeErr *multierror.Error
	done := make(chan struct{})
	go func() {
		defer close(done)

		for batch := range metadataChan {
			dr.PebbleLock.Lock()
			if batch.Err != nil {
				writeErr = multierror.Append(writeErr, batch.Err)
				dr.PebbleLock.Unlock()
				continue
			}

			// Set Indices (forward and reverse pre-constructed in table goroutine)
			for _, entry := range batch.Indices {
				if err := tx.Set(entry.Key, entry.Value, nil); err != nil {
					writeErr = multierror.Append(writeErr, err)
				}
			}

			// Set Location metadata
			for _, entry := range batch.Metadata {
				dr.LocCache.Set(string(entry.Id), entry.Loc)
				if err := dr.AddTableEntryInfo(tx, entry.Id, entry.Loc); err != nil {
					writeErr = multierror.Append(writeErr, err)
				}
			}
			dr.PebbleLock.Unlock()
		}
	}()

	wg.Wait()
	close(metadataChan)
	<-done

	return writeErr.ErrorOrNil()
}
