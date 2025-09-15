package table

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/tpath"
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
			wg.Done()
		}()

		var allFieldIndexKeyElements = make([]FieldKeyElements, 0, batchSize*len(b.Fields)) // Pre-allocate
		allMetadata := make(map[string]*benchtop.RowLoc, batchSize)
		var localErr *multierror.Error

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
						if val := tpath.PathLookup(row.Data, field); val != nil { // Assumes PathLookup exists
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
				var totalDataSize uint32

				for _, row := range rowsInPartition {
					bData, err := sonic.ConfigFastest.Marshal(b.PackData(row.Data, string(row.Id)))
					if err != nil {
						localErr = multierror.Append(localErr, fmt.Errorf("marshal error for row %s: %v", row.Id, err))
						continue
					}
					bDatas = append(bDatas, bData)
					rowIds = append(rowIds, string(row.Id))
					totalDataSize += uint32(len(bData)) + benchtop.ROW_HSIZE
				}
				if len(bDatas) == 0 {
					continue
				}

				// Get the current active section for this partition.
				b.SectionLock.Lock()
				secId := b.PartitionMap[partitionId][len(b.PartitionMap[partitionId])-1]
				sec := b.Sections[secId]
				b.SectionLock.Unlock()

				sec.Lock.Lock()
				if sec.LiveBytes+totalDataSize > MAX_SECTION_SIZE {
					sec.Lock.Unlock() // Unlock old section

					newSec, err := b.CreateNewSection(partitionId)
					if err != nil {
						localErr = multierror.Append(localErr, fmt.Errorf("failed to create new section for partition %d", partitionId))
						continue
					}
					sec = newSec
					sec.Lock.Lock() // Lock new section
				}

				startOffset, err := sec.Handle.Seek(0, io.SeekEnd)
				if err != nil {
					localErr = multierror.Append(localErr, fmt.Errorf("seek error for section %d: %v", sec.ID, err))
					sec.Lock.Unlock()
					continue
				}

				batchPayload := make([]byte, totalDataSize)
				currentPos, currentOffset := 0, uint32(startOffset)
				for i, bData := range bDatas {
					nextOffset := currentOffset + benchtop.ROW_HSIZE + uint32(len(bData))
					binary.LittleEndian.PutUint32(batchPayload[currentPos:currentPos+int(benchtop.ROW_OFFSET_HSIZE)], nextOffset)
					binary.LittleEndian.PutUint32(batchPayload[currentPos+int(benchtop.ROW_OFFSET_HSIZE):currentPos+int(benchtop.ROW_HSIZE)], uint32(len(bData)))
					copy(batchPayload[currentPos+int(benchtop.ROW_HSIZE):], bData)

					allMetadata[rowIds[i]] = &benchtop.RowLoc{
						TableId: b.TableId,
						Section: sec.ID,
						Offset:  currentOffset,
						Size:    uint32(len(bData)),
					}
					currentOffset = nextOffset
					currentPos += int(benchtop.ROW_HSIZE) + len(bData)
				}

				if _, err := sec.Handle.Write(batchPayload); err != nil {
					localErr = multierror.Append(localErr, fmt.Errorf("write error for section %d: %v", sec.ID, err))
					sec.Lock.Unlock()
					continue
				}
				sec.TotalRows += uint32(len(bDatas))
				sec.LiveBytes += totalDataSize
				sec.Lock.Unlock()
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
