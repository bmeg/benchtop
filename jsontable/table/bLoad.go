package table

import (
	"fmt"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/section"
	"github.com/bmeg/benchtop/jsontable/tpath"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
	multierror "github.com/hashicorp/go-multierror"
)

type FieldKeyElements struct {
	Field     string
	TableName string
	Val       any
	RowId     string
}

type KitchenSink struct {
	FieldIndexKeyElements []FieldKeyElements
	Metadata              map[string]*benchtop.RowLoc
	Err                   error
}

func (b *JSONTable) StartTableGoroutine(
	wg *sync.WaitGroup,
	metadataChan chan *KitchenSink,
	snapshot *pebble.Snapshot,
	batchSize int,
) chan *benchtop.Row {
	ch := make(chan *benchtop.Row, batchSize)
	wg.Add(1)
	go func() {
		defer func() {
			// --- FINAL FLUSH ON EXIT ---
			b.SectionLock.Lock()
			for _, sec := range b.ActiveSections {
				if sec.LiveBytes > 0 {
					if err := sec.MMap.Flush(); err != nil {
						log.Errorf("Final flush failed for section %d: %v", sec.ID, err)
					}
					err := sec.File.Sync()
					if err != nil {
						log.Errorf("File Sync failed in bulk load: %v", err)
					}
				}
			}
			b.SectionLock.Unlock()
			wg.Done()
		}()

		const FLUSH_EVERY = 1000
		var allFieldIndexKeyElements = make([]FieldKeyElements, 0, batchSize*len(b.Fields))
		allMetadata := make(map[string]*benchtop.RowLoc, batchSize)
		var localErr *multierror.Error

		var flushCounter uint32 = 0
		for {
			batch := make([]*benchtop.Row, 0, batchSize)
			for range batchSize {
				row, ok := <-ch
				if !ok {
					break
				}
				batch = append(batch, row)
			}
			if len(batch) == 0 {
				break
			}

			newRows := make([]*benchtop.Row, 0, len(batch))
			for _, row := range batch {
				info, err := b.GetTableEntryInfo(snapshot, row.Id)
				if err != nil {
					localErr = multierror.Append(localErr, fmt.Errorf("error getting entry info for %s: %v", row.Id, err))
					continue
				}
				if info == nil {
					newRows = append(newRows, row)
					for field := range b.Fields {
						if val := tpath.PathLookup(row.Data, field); val != nil {
							allFieldIndexKeyElements = append(allFieldIndexKeyElements, FieldKeyElements{
								Field:     field,
								TableName: b.Name,
								Val:       val,
								RowId:     string(row.Id),
							})
						}
					}
				}
			}

			if len(newRows) == 0 {
				continue
			}

			rowsByPartition := make(map[uint8][]*benchtop.Row)
			for _, row := range newRows {
				partitionId := b.PartitionFunc(row.Id)
				rowsByPartition[partitionId] = append(rowsByPartition[partitionId], row)
			}

			for partitionId, rowsInPartition := range rowsByPartition {
				if len(rowsInPartition) == 0 {
					continue
				}

				bDatas := make([][]byte, 0, len(rowsInPartition))
				rowIds := make([]string, 0, len(rowsInPartition))
				var totalUncompressedSize uint32

				for _, row := range rowsInPartition {
					bData, err := sonic.ConfigFastest.Marshal(b.PackData(row.Data, string(row.Id)))
					if err != nil {
						localErr = multierror.Append(localErr, fmt.Errorf("marshal error for row %s: %v", row.Id, err))
						continue
					}
					bDatas = append(bDatas, bData)
					rowIds = append(rowIds, string(row.Id))
					totalUncompressedSize += uint32(len(bData)) + 8
				}
				if len(bDatas) == 0 {
					continue
				}

				sec := b.ActiveSections[partitionId] // This is the section active for writing
				if sec == nil {
					// This should not happen if Init is correct, but add recovery/guard
					var err error
					sec, err = b.CreateNewSection(partitionId)
					if err != nil {
						localErr = multierror.Append(localErr, fmt.Errorf("failed to get or create active section for partition %d: %v", partitionId, err))
						continue
					}
				}

				// --- ROTATE SECTION IF FULL ---
				if sec.LiveBytes+totalUncompressedSize > section.MAX_SECTION_SIZE {
					// Flush old section before rotating
					if sec.LiveBytes > 0 {
						err := sec.CloseSection()
						if err != nil {
							localErr = multierror.Append(localErr, err)
						}
					}

					newSec, err := b.CreateNewSection(partitionId)
					if err != nil {
						localErr = multierror.Append(localErr, fmt.Errorf("failed to create new section for partition %d: %v", partitionId, err))
						continue
					}
					sec = newSec
				}

				for i, bData := range bDatas {
					rowLoc, err := sec.WriteJsonEntryToSection(bData)
					if err != nil {
						localErr = multierror.Append(localErr, fmt.Errorf("write error for row %s in section %d: %v", rowIds[i], sec.ID, err))
						continue
					}
					rowLoc.TableId = b.TableId
					allMetadata[rowIds[i]] = rowLoc

					// --- PERIODIC FLUSH ---
					flushCounter++
					/*if flushCounter >= FLUSH_EVERY {
					sec.Lock.Lock()
					if err := sec.MMap.Flush(); err != nil {
						log.Errorf("Periodic flush failed for section %d: %v", sec.ID, err)
					}
					sec.Lock.Unlock()
					flushCounter = 0
					}*/
				}
				sec.TotalRows += uint32(len(bDatas))
			}
		}

		metadataChan <- &KitchenSink{
			FieldIndexKeyElements: allFieldIndexKeyElements,
			Metadata:              allMetadata,
			Err:                   localErr.ErrorOrNil(),
		}
	}()
	return ch
}
