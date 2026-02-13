package table

import (
	"fmt"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/tpath"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	multierror "github.com/hashicorp/go-multierror"
)

type IndexEntry struct {
	Key   []byte
	Value []byte
}

type MetadataEntry struct {
	Id  []byte
	Loc *benchtop.RowLoc
}

type IngestBatch struct {
	Indices  []IndexEntry
	Metadata []MetadataEntry
	Err      error
}

func (b *JSONTable) StartTableGoroutine(
	wg *sync.WaitGroup,
	metadataChan chan *IngestBatch,
	batchSize int,
) chan *benchtop.Row {
	ch := make(chan *benchtop.Row, batchSize)
	wg.Add(1)
	go func() {
		defer func() {
			if err := b.Storage.Sync(); err != nil {
				log.Errorf("Final sync failed: %v", err)
			}
			wg.Done()
		}()

		indices := make([]IndexEntry, 0, batchSize*2)
		metadata := make([]MetadataEntry, 0, batchSize)
		var localErr *multierror.Error

		flush := func() {
			if len(metadata) > 0 || localErr != nil {
				metadataChan <- &IngestBatch{
					Indices:  indices,
					Metadata: metadata,
					Err:      localErr.ErrorOrNil(),
				}
				indices = make([]IndexEntry, 0, batchSize*2)
				metadata = make([]MetadataEntry, 0, batchSize)
				localErr = nil
			}
		}

		rowsBatch := make([]benchtop.Row, 0, batchSize)

		processBatch := func(rows []benchtop.Row) {
			if len(rows) == 0 {
				return
			}
			locs, err := b.AddRows(rows)
			if err != nil {
				// Record error for all rows? Or trying to continue?
				// AddRows is atomic per batch usually.
				// If error, likely fatal for the batch.
				// We can append global error.
				localErr = multierror.Append(localErr, fmt.Errorf("AddRows error: %v", err))
				return
			}

			if len(locs) != len(rows) {
				localErr = multierror.Append(localErr, fmt.Errorf("AddRows returned %d locs for %d rows", len(locs), len(rows)))
				return
			}

			for i, row := range rows {
				loc := locs[i]
				bLoc := &benchtop.RowLoc{
					TableId: b.TableId,
					Section: loc.Section,
					Offset:  loc.Offset,
					Size:    loc.Size,
					Index:   loc.Index,
				}
				metadata = append(metadata, MetadataEntry{Id: row.Id, Loc: bLoc})

				// Generate index entries parallelly
				for field := range b.Fields {
					if val := tpath.PathLookup(row.Data, field); val != nil {
						fKey := benchtop.FieldKey(field, b.Name, val, row.Id)
						indices = append(indices, IndexEntry{Key: fKey, Value: []byte{}})

						rKey := benchtop.RFieldKey(b.Name, field, string(row.Id))
						bVal, err := sonic.ConfigFastest.Marshal(val)
						if err == nil {
							indices = append(indices, IndexEntry{Key: rKey, Value: bVal})
						}
					}
				}
			}
			if len(metadata) >= batchSize {
				flush()
			}
		}

		for row := range ch {
			rowsBatch = append(rowsBatch, *row)
			if len(rowsBatch) >= batchSize {
				processBatch(rowsBatch)
				rowsBatch = rowsBatch[:0]
			}
		}
		if len(rowsBatch) > 0 {
			processBatch(rowsBatch)
		}
		flush()
	}()
	return ch
}
