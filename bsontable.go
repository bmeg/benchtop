package benchtop

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

	"github.com/bmeg/grip/log"

	"github.com/cockroachdb/pebble"

	"go.mongodb.org/mongo-driver/bson"
	//NOTE: try github.com/dgraph-io/ristretto for cache
)

type BSONTable struct {
	columns    []ColumnDef
	columnMap  map[string]int
	handle     *os.File
	db         *pebble.DB
	tableId    uint32
	handleLock sync.RWMutex
	path       string
}

type dbSet interface {
	Set(id []byte, val []byte, opts *pebble.WriteOptions) error
}

type dbGet interface {
	Get(key []byte) ([]byte, io.Closer, error)
}

type dbDelete interface {
	Delete(key []byte, _ *pebble.WriteOptions) error
}

func (b *BSONTable) Close() {
	//because the table could be opened by other threads, don't actually close
	//b.handle.Close()
	//b.db.Close()
}

func (b *BSONTable) GetColumns() []ColumnDef {
	return b.columns
}

/*
////////////////////////////////////////////////////////////////
Unary single effect operations
*/
func (b *BSONTable) Add(id []byte, entry map[string]any) error {
	dData, err := b.packData(entry, string(id))
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

	offsetBuffer := make([]byte, 8)
	writesize, err := b.WriteOffset(offsetBuffer, offset, bData)
	if err != nil {
		log.Errorf("write handler err in Load: bulkSet: %s", err)
	}
	b.addTableEntryInfo(b.db, id, uint64(offset), uint64(writesize))

	return nil
}

func (b *BSONTable) Get(id []byte, fields ...string) (map[string]any, error) {
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

func (b *BSONTable) Delete(name []byte) error {
	offset, _, err := b.getBlockPos(name)
	if err != nil {
		return err
	}
	b.handle.Seek(int64(offset+8), io.SeekStart)
	_, err = b.handle.Write([]byte{0x00, 0x00, 0x00, 0x00})
	if err != nil {
		return err
	}

	posKey := NewPosKey(b.tableId, name)
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

	oldHandle := b.handle
	_, err = oldHandle.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	reader := bufio.NewReaderSize(oldHandle, 16*1024*1024)
	writer := bufio.NewWriterSize(tempHandle, 16*1024*1024)
	defer writer.Flush()

	//newOffsets := make(map[string]uint64)
	var newOffset uint64 = 0

	offsetSizeData := make([]byte, 8)
	sizeBytes := make([]byte, 4)

	rowBuff := make([]byte, 0, 1<<20)

	fileOffset := int64(0)
	inputChan := make(chan Index, 100)

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
		if bSize == 0 {
			// Check if there's a gap
			if int64(nextOffset) > fileOffset {
				startSeek := time.Now()
				_, err = oldHandle.Seek(int64(nextOffset), io.SeekStart)
				if err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("failed to seek to nextOffset: %w", err)
				}
				fileOffset = int64(nextOffset) // Update fileOffset
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

		inputChan <- Index{Key: []byte(val.StringValue()), Position: newOffset}
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
			writer.Flush()
		}

		log.Debugf("Time to write new offset and BSON: %v\n", time.Since(startWrite))

		newOffset += uint64(len(rowBuff)) + 8

		log.Debugf("Total loop iteration time: %v\n", time.Since(startLoop))
	}
	close(inputChan)
	wg.Wait()

	// Replace the old file with the compacted one
	fileName, err := filepath.Abs(b.handle.Name())
	if err != nil {
		return err
	}

	tempHandle.Sync()

	err = os.Rename(tempFileName, fileName)
	if err != nil {
		return fmt.Errorf("failed renaming compacted file: %w", err)
	}

	b.handle, err = os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed reopening compacted file: %w", err)
	}

	return nil
}

/*
////////////////////////////////////////////////////////////////
Start of bulk, chan based functions
*/
func (b *BSONTable) Keys() (chan Index, error) {
	out := make(chan Index, 10)
	go func() {
		defer close(out)

		prefix := NewPosKeyPrefix(b.tableId)
		it, err := b.db.NewIter(&pebble.IterOptions{})
		if err != nil {
			log.Errorf("error: %s", err)
		}
		for it.SeekGE(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			_, value := ParsePosKey(it.Key())
			out <- Index{Key: value}
		}
		it.Close()
	}()
	return out, nil
}

func (b *BSONTable) Scan(keys bool, filter []FieldFilter, fields ...string) (chan map[string]any, error) {
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

			// Elem has been deleted. skip it.
			if bSize == 0 {
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

			for _, colName := range fields {
				if i, ok := b.columnMap[colName]; ok {
					n := b.columns[i]
					unpack := b.colUnpack(columns.Index(uint(i)), n.Type)
					if PassesFilters(unpack, filter) {
						vOut[n.Name] = unpack
						if keys {
							vOut["_key"] = bd.Index(2).Value().StringValue()
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

func (b *BSONTable) Fetch(inputs chan Index, workers int) <-chan BulkResponse {
	results := make(chan BulkResponse, workers)

	var wg sync.WaitGroup
	go func() {
		b.bulkGet(func(s dbGet) error {
			for entry := range inputs {

				wg.Add(1)
				go func(index Index) {
					defer wg.Done()

					// Get offset from Pebble batch
					val, closer, err := s.Get(NewPosKey(b.tableId, index.Key))
					if err != nil {
						results <- BulkResponse{string(index.Key), nil, func() string {
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
					results <- BulkResponse{string(index.Key), data, func() string {
						if err != nil {
							return err.Error()
						}
						return ""
					}()}

				}(entry)
			}
			return nil
		})

		wg.Wait()
		close(results)
	}()

	return results
}

func (b *BSONTable) Load(inputs chan Entry) error {
	b.handleLock.Lock()
	defer b.handleLock.Unlock()
	offset, err := b.handle.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	bsonHandleNextoffset := make([]byte, 8)
	b.bulkSet(func(s dbSet) error {
		for entry := range inputs {
			dData, err := b.packData(entry.Value, string(entry.Key))
			if err != nil {
				log.Errorf("pack data err in Load: bulkSet: %s", err)
			}
			bData, err := bson.Marshal(dData)
			if err != nil {
				log.Errorf("bson Marshall err in Load: bulkSet: %s", err)
			}

			// make Next offset equal to existing offset + length of data
			writeSize, err := b.WriteOffset(bsonHandleNextoffset, offset, bData)
			if err != nil {
				log.Errorf("write handler err in Load: bulkSet: %s", err)
			}
			b.addTableEntryInfo(s, entry.Key, uint64(offset), uint64(writeSize))
			offset += int64(writeSize) + 8
		}
		return nil
	})

	return nil
}

func (b *BSONTable) Remove(inputs chan Index, workers int) <-chan BulkResponse {
	results := make(chan BulkResponse, workers)

	batchDeletes := make(chan Index, workers)

	go func() {
		b.bulkDelete(func(s dbDelete) error {
			for index := range batchDeletes {
				err := s.Delete(NewPosKey(b.tableId, index.Key), nil)
				if err != nil {
					results <- BulkResponse{string(index.Key), nil, func() string {
						if err != nil {
							return err.Error()
						}
						return ""
					}()}
				}
			}
			return nil
		})
		close(results)
	}()

	var wg sync.WaitGroup
	go func() {
		defer close(batchDeletes)
		b.bulkGet(func(s dbGet) error {
			for index := range inputs {
				wg.Add(1)
				go func(index Index) {
					defer wg.Done()

					key := string(index.Key)
					val, closer, err := s.Get(NewPosKey(b.tableId, index.Key))
					if err != nil {
						results <- BulkResponse{key, nil, func() string {
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
						results <- BulkResponse{key, nil, func() string {
							if err != nil {
								return err.Error()
							}
							return ""
						}()}
						return
					}

					batchDeletes <- index
					results <- BulkResponse{key, nil, ""}
				}(index)
			}
			return nil
		})
		wg.Wait()
	}()

	return results
}
