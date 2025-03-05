package bsontable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable/filters"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
	multierror "github.com/hashicorp/go-multierror"

	"github.com/cockroachdb/pebble"

	"go.mongodb.org/mongo-driver/bson"
	//NOTE: try github.com/dgraph-io/ristretto for cache
)

type BSONTable struct {
	Pb         *pebblebulk.PebbleKV
	db         *pebble.DB
	columns    []benchtop.ColumnDef
	columnMap  map[string]int
	handle     *os.File
	tableId    uint32
	handleLock sync.RWMutex
	Path       string
	tType      byte
}

func (b *BSONTable) GetColumnDefs() []benchtop.ColumnDef {
	return b.columns
}

func (b *BSONTable) Close() {
	//because the table could be opened by other threads, don't actually close
	//b.handle.Close()
	//b.db.Close()
}

/*
////////////////////////////////////////////////////////////////
Unary single effect operations
*/
func (b *BSONTable) AddRow(elem benchtop.Row) error {
	dData, err := b.packData(elem.Data, string(elem.Id))
	if err != nil {
		return err
	}

	bData, err := bson.Marshal(dData)
	if err != nil {
		return err
	}

	//append to end of block file
	b.handleLock.Lock()
	defer b.handleLock.Unlock()
	offset, err := b.handle.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	writesize, err := b.writeBsonEntry(offset, bData)
	if err != nil {
		log.Errorf("write handler err in Load: bulkSet: %s", err)
	}

	b.addTableEntryInfo(nil, elem.Id, elem.Label, uint64(offset), uint64(writesize))
	return nil

}

func (b *BSONTable) GetRow(id []byte, fields ...string) (map[string]any, error) {
	b.handleLock.RLock()
	defer b.handleLock.RUnlock()

	offset, _, err := b.getBlockPos(id)
	if err != nil {
		return nil, err
	}

	// Offset skip the first 8 bytes since they are for getting the offset for a scan operation
	b.handle.Seek(int64(offset+8), io.SeekStart)
	//The next 4 bytes of the BSON block is the size
	sizeBytes := []byte{0x00, 0x00, 0x00, 0x00}
	_, err = b.handle.Read(sizeBytes)
	if err != nil {
		return nil, err
	}
	bSize := int32(binary.LittleEndian.Uint32(sizeBytes))
	b.handle.Seek(-4, io.SeekCurrent)
	rowData := make([]byte, bSize)
	b.handle.Read(rowData)
	bd := bson.Raw(rowData)

	columns, ok := bd.Index(0).Value().ArrayOK()
	if !ok || len(columns) == 0 {
		return nil, fmt.Errorf("'columns' array is missing or empty")
	}

	out := map[string]any{}
	if len(fields) == 0 {
		if err := bd.Index(1).Value().Unmarshal(&out); err != nil {
			return nil, err
		}
		elem, err := columns.Elements()
		if err != nil {
			return nil, err
		}
		for i, n := range b.columns {
			out[n.Name] = b.colUnpack(elem[i], n.Type)
		}
	} else {
		for _, colName := range fields {
			if i, ok := b.columnMap[colName]; ok {
				n := b.columns[i]
				elem := columns.Index(uint(i))
				out[n.Name] = b.colUnpack(elem, n.Type)
			}
		}
	}
	return out, nil
}

func (b *BSONTable) DeleteRow(name []byte) error {
	offset, _, err := b.getBlockPos(name)
	if err != nil {
		return err
	}
	b.handle.Seek(int64(offset+8), io.SeekStart)
	_, err = b.handle.Write([]byte{0x00, 0x00, 0x00, 0x00})
	if err != nil {
		return err
	}

	posKey := benchtop.NewPosKey(b.tableId, name)
	b.db.Delete(posKey, nil)

	return nil
}

func (b *BSONTable) Compact() error {
	const flushThreshold = 1000
	flushCounter := 0
	b.handleLock.Lock()
	defer b.handleLock.Unlock()

	tempFileName, err := filepath.Abs(b.handle.Name() + ".compact")
	if err != nil {
		return err
	}

	tempHandle, err := os.Create(tempFileName)
	if err != nil {
		return err
	}
	defer tempHandle.Close() // Ensure tempHandle is closed on function exit

	oldHandle := b.handle
	_, err = oldHandle.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	defer oldHandle.Close() // Ensure oldHandle is closed on function exit

	reader := bufio.NewReaderSize(oldHandle, 16*1024*1024)
	writer := bufio.NewWriterSize(tempHandle, 16*1024*1024)

	var newOffset uint64 = 0
	offsetSizeData := make([]byte, 8)
	sizeBytes := make([]byte, 4)
	rowBuff := make([]byte, 0, 1<<20)

	fileOffset := int64(0)
	inputChan := make(chan benchtop.Index, 100)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.setIndices(inputChan)
	}()

	for {
		startLoop := time.Now()

		// --- Read next offset ---
		startReadOffset := time.Now()
		_, err := io.ReadFull(reader, offsetSizeData)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed reading next offset: %w", err)
		}
		nextOffset := binary.LittleEndian.Uint64(offsetSizeData)
		log.Debugf("Time to read offset: %v\n", time.Since(startReadOffset))

		// --- Read BSON object size (4 bytes) ---
		startReadSize := time.Now()
		_, err = io.ReadFull(reader, sizeBytes)
		if err != nil {
			return fmt.Errorf("failed reading size: %w", err)
		}
		bSize := int32(binary.LittleEndian.Uint32(sizeBytes))
		log.Debugf("Time to read BSON size: %v\n", time.Since(startReadSize))

		// --- Handle empty records ---
		fileOffset += 8 + 4
		if bSize == 0 || fileOffset == int64(12) {
			if int64(nextOffset) > fileOffset {
				startSeek := time.Now()
				_, err = oldHandle.Seek(int64(nextOffset), io.SeekStart)
				if err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("failed to seek to nextOffset: %w", err)
				}
				fileOffset = int64(nextOffset)
				reader.Reset(oldHandle)
				log.Debugf("Time to seek & reset reader: %v\n", time.Since(startSeek))
			}
			continue
		}

		startReadBSON := time.Now()
		if int(bSize) > cap(rowBuff) {
			rowBuff = make([]byte, bSize)
		} else {
			rowBuff = rowBuff[:bSize]
		}
		copy(rowBuff, sizeBytes)
		_, err = io.ReadFull(reader, rowBuff[4:])
		if err != nil {
			return fmt.Errorf("failed reading BSON data: %w", err)
		}
		log.Debugf("Time to read BSON record: %v\n", time.Since(startReadBSON))

		// --- Extract 'columns' field from BSON ---
		startParseBSON := time.Now()
		raw := bson.Raw(rowBuff)
		val, exists := raw.LookupErr("key")
		if exists != nil {
			return fmt.Errorf("'columns' field not found in BSON document")
		}

		inputChan <- benchtop.Index{Key: []byte(val.StringValue()), Position: newOffset}
		log.Debugf("Time to parse BSON 'columns': %v\n", time.Since(startParseBSON))

		// --- Write new offset ---
		startWrite := time.Now()
		newOffsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(newOffsetBytes, newOffset+uint64(len(rowBuff))+8)

		_, err = writer.Write(newOffsetBytes)
		if err != nil {
			return fmt.Errorf("failed writing new offset: %w", err)
		}
		_, err = writer.Write(rowBuff)
		if err != nil {
			return fmt.Errorf("failed writing BSON row: %w", err)
		}

		flushCounter++
		if flushCounter%flushThreshold == 0 {
			if err := writer.Flush(); err != nil {
				return fmt.Errorf("failed flushing writer: %w", err)
			}
		}

		log.Debugf("Time to write new offset and BSON: %v\n", time.Since(startWrite))

		newOffset += uint64(len(rowBuff)) + 8

		log.Debugf("Total loop iteration time: %v\n", time.Since(startLoop))
	}
	close(inputChan)
	wg.Wait()

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed final flush of writer: %w", err)
	}

	if err := tempHandle.Sync(); err != nil {
		return fmt.Errorf("failed syncing temp file: %w", err)
	}
	if err := tempHandle.Close(); err != nil {
		return fmt.Errorf("failed closing temp file: %w", err)
	}

	if err := oldHandle.Close(); err != nil {
		return fmt.Errorf("failed closing old handle: %w", err)
	}

	fileName, err := filepath.Abs(b.handle.Name())
	if err != nil {
		return err
	}

	if err := os.Rename(tempFileName, fileName); err != nil {
		return fmt.Errorf("failed renaming compacted file: %w", err)
	}

	newHandle, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed reopening compacted file: %w", err)
	}
	b.handle = newHandle

	return nil
}

/*
////////////////////////////////////////////////////////////////
Start of bulk, chan based functions
*/
func (b *BSONTable) Keys() (chan benchtop.Index, error) {
	out := make(chan benchtop.Index, 10)
	go func() {
		defer close(out)

		prefix := benchtop.NewPosKeyPrefix(b.tableId)
		it, err := b.db.NewIter(&pebble.IterOptions{})
		if err != nil {
			log.Errorf("error: %s", err)
		}
		for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			_, value := benchtop.ParsePosKey(it.Key())
			out <- benchtop.Index{Key: value}
		}
		it.Close()
	}()
	return out, nil
}

func (b *BSONTable) Scan(keys bool, filter []benchtop.FieldFilter, fields ...string) (chan map[string]any, error) {
	b.handleLock.RLock()
	defer b.handleLock.RUnlock()

	out := make(chan map[string]any, 10)
	_, err := b.handle.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(out)
		for {
			offsetSizeData := make([]byte, 8)
			_, err := b.handle.Read(offsetSizeData)
			if err == io.EOF {
				break
			}
			if err != nil {
				return
			}

			NextOffset := binary.LittleEndian.Uint64(offsetSizeData)

			sizeBytes := make([]byte, 4)
			_, err = b.handle.Read(sizeBytes)
			if err != nil {
				return
			}

			bSize := int32(binary.LittleEndian.Uint32(sizeBytes))

			// Elem has been deleted or at the table header in the begginning of the file skip it.
			if bSize == 0 || int64(bSize) == int64(NextOffset)-8 {
				_, err = b.handle.Seek(int64(NextOffset), io.SeekStart)
				if err == io.EOF {
					break
				}
				continue
			}
			rowData := make([]byte, bSize)
			copy(rowData, sizeBytes)

			_, err = b.handle.Read(rowData[4:])
			if err != nil {
				return
			}

			bd := bson.Raw(rowData)
			columns := bd.Index(0).Value().Array()

			vOut := map[string]any{}

			if len(fields) == 0 {
				if keys {
					vOut["_key"] = bd.Index(2).Value().StringValue()
				}
			} else {
				for _, colName := range fields {
					if i, ok := b.columnMap[colName]; ok {
						n := b.columns[i]
						unpack := b.colUnpack(columns.Index(uint(i)), n.Type)
						if filters.PassesFilters(unpack, filter) {
							vOut[n.Name] = unpack
							if keys {
								vOut["_key"] = bd.Index(2).Value().StringValue()
							}
						}
					}
				}
			}
			if len(vOut) > 0 {
				out <- vOut
			}

			_, err = b.handle.Seek(int64(NextOffset), io.SeekStart)
			if err == io.EOF {
				break
			}
		}
	}()
	return out, nil
}

func (b *BSONTable) Fetch(inputs chan benchtop.Index, workers int) <-chan benchtop.BulkResponse {
	results := make(chan benchtop.BulkResponse, workers)
	var wg sync.WaitGroup
	go func() {
		for entry := range inputs {
			wg.Add(1)
			go func(index benchtop.Index) {
				defer wg.Done()
				val, closer, err := b.db.Get(benchtop.NewPosKey(b.tableId, index.Key))
				if err != nil {
					results <- benchtop.BulkResponse{Key: string(index.Key), Data: nil, Err: func() string {
						if err != nil {
							return err.Error()
						}
						return ""
					}()}
					return
				}
				defer closer.Close()

				data, err := b.readFromFile(binary.LittleEndian.Uint64(val))
				if err != nil {
					data = nil
				}

				results <- benchtop.BulkResponse{Key: string(index.Key), Data: data, Err: func() string {
					if err != nil {
						return err.Error()
					}
					return ""
				}()}

			}(entry)
		}
		wg.Wait()
		close(results)
	}()
	return results
}

func (b *BSONTable) Load(inputs chan benchtop.Row) error {
	var errs *multierror.Error
	b.handleLock.Lock()
	defer b.handleLock.Unlock()
	offset, err := b.handle.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	err = b.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		for entry := range inputs {
			dData, err := b.packData(entry.Data, string(entry.Id))
			if err != nil {
				errs = multierror.Append(errs, err)
				log.Errorf("pack data err in Load: bulkSet: %s", err)
			}
			bData, err := bson.Marshal(dData)
			if err != nil {
				errs = multierror.Append(errs, err)
				log.Errorf("bson Marshall err in Load: bulkSet: %s", err)
			}

			// make Next offset equal to existing offset + length of data
			writeSize, err := b.writeBsonEntry(offset, bData)
			if err != nil {
				errs = multierror.Append(errs, err)
				log.Errorf("write handler err in Load: bulkSet: %s", err)
			}
			b.addTableEntryInfo(tx, entry.Id, entry.Label, uint64(offset), uint64(writeSize))
			offset += int64(writeSize) + 8
		}
		return nil
	})
	if err != nil {
		log.Errorf("Err: %s", err)
		errs = multierror.Append(errs, err)
	}
	return errs.ErrorOrNil()

}

func (b *BSONTable) Remove(inputs chan benchtop.Index, workers int) <-chan benchtop.BulkResponse {
	results := make(chan benchtop.BulkResponse, workers)
	batchDeletes := make(chan benchtop.Index, workers)

	go func() {
		for index := range batchDeletes {
			err := b.db.Delete(benchtop.NewPosKey(b.tableId, index.Key), nil)
			if err != nil {
				results <- benchtop.BulkResponse{Key: string(index.Key), Data: nil, Err: func() string {
					if err != nil {
						return err.Error()
					}
					return ""
				}()}
			}
		}

		close(results)
	}()

	var wg sync.WaitGroup
	go func() {
		defer close(batchDeletes)
		for index := range inputs {
			wg.Add(1)
			go func(index benchtop.Index) {
				defer wg.Done()

				key := string(index.Key)
				val, closer, err := b.db.Get(benchtop.NewPosKey(b.tableId, index.Key))
				if err != nil {
					results <- benchtop.BulkResponse{Key: key, Data: nil, Err: func() string {
						if err != nil {
							return err.Error()
						}
						return ""
					}()}
					return
				}
				defer closer.Close()

				offset := binary.LittleEndian.Uint64(val)
				if err := b.markDelete(offset); err != nil {
					results <- benchtop.BulkResponse{Key: key, Data: nil, Err: func() string {
						if err != nil {
							return err.Error()
						}
						return ""
					}()}
					return
				}

				batchDeletes <- index
				results <- benchtop.BulkResponse{Key: key, Data: nil, Err: ""}
			}(index)
		}
		wg.Wait()
	}()

	return results
}
