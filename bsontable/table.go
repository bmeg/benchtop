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
	Name       string
	filePool   chan *os.File
	FileName   string
}

func (b *BSONTable) Init(poolSize int) error {
	b.filePool = make(chan *os.File, poolSize)
	for range 10 {
		file, err := os.Open(b.Path)
		if err != nil {
			return fmt.Errorf("failed to init file pool for %s: %v", b.Path, err)
		}
		b.filePool <- file
	}
	return nil
}

func (b *BSONTable) GetColumnDefs() []benchtop.ColumnDef {
	return b.columns
}

func (b *BSONTable) Close() {
	//because the table could be opened by other threads, don't actually close
}

/*
////////////////////////////////////////////////////////////////
Unary single effect operations
*/
func (b *BSONTable) AddRow(elem benchtop.Row, tx *pebblebulk.PebbleBulk) error {
	mData, err := b.packData(elem.Data, string(elem.Id))
	if err != nil {
		return err
	}

	bData, err := bson.Marshal(mData)
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
	b.addTableDeleteEntryInfo(tx, elem.Id, elem.TableName)
	b.addTableEntryInfo(tx, elem.Id, uint64(offset), uint64(writesize))

	return nil
}

func (b *BSONTable) GetRow(id []byte, fields ...string) (map[string]any, error) {
	file := <-b.filePool
	defer func() {
		file.Seek(0, io.SeekStart)
		b.filePool <- file
	}()

	offset, size, err := b.getBlockPos(id)
	if err != nil {
		return nil, err
	}
	// Offset skip the first 8 bytes since they are for getting the offset for a scan operation
	if _, err := file.Seek(int64(offset+8), io.SeekStart); err != nil {
		return nil, err
	}

	rowData := make([]byte, size)
	if _, err := io.ReadFull(file, rowData); err != nil {
		return nil, err
	}

	var m bson.M
	if err := bson.Unmarshal(rowData, &m); err == nil {
		if len(m) > 0 {
			out, err := b.unpackData(m)
			if err != nil {
				return nil, err
			}
			return out, nil
		}
	}
	return nil, err
}

func (b *BSONTable) DeleteRow(name []byte) error {
	offset, _, err := b.getBlockPos(name)
	if err != nil {
		return err
	}
	b.handleLock.Lock()
	if _, err := b.handle.WriteAt([]byte{0x00, 0x00, 0x00, 0x00}, int64(offset+8)); err != nil {
		return fmt.Errorf("writeAt failed: %w", err)
	}
	b.handleLock.Unlock()
	b.db.Delete(benchtop.NewPosKey(b.tableId, name), nil)
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
	defer tempHandle.Close()

	oldHandle := b.handle
	_, err = oldHandle.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	defer oldHandle.Close()

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
		b.setDataIndices(inputChan)
	}()

	for {
		_, err := io.ReadFull(reader, offsetSizeData)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed reading next offset: %w", err)
		}
		nextOffset := binary.LittleEndian.Uint64(offsetSizeData)

		_, err = io.ReadFull(reader, sizeBytes)
		if err != nil {
			return fmt.Errorf("failed reading size: %w", err)
		}
		bSize := int32(binary.LittleEndian.Uint32(sizeBytes))

		fileOffset += 8 + 4
		if bSize == 0 || fileOffset == int64(12) {
			if int64(nextOffset) > fileOffset {
				_, err = oldHandle.Seek(int64(nextOffset), io.SeekStart)
				if err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("failed to seek to nextOffset: %w", err)
				}
				fileOffset = int64(nextOffset)
				reader.Reset(oldHandle)
			}
			continue
		}

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

		val := bson.Raw(rowBuff).Lookup("R").Array().Index(2).Value()
		inputChan <- benchtop.Index{Key: []byte(val.StringValue()), Position: newOffset, Size: uint64(bSize)}

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

		newOffset += uint64(len(rowBuff)) + 8
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

	oldPool := b.filePool
	b.filePool = make(chan *os.File, cap(oldPool))
	for i := 0; i < cap(oldPool); i++ {
		file, err := os.Open(b.Path)
		if err != nil {
			return fmt.Errorf("failed to refresh file pool: %v", err)
		}
		b.filePool <- file
	}
	close(oldPool)
	for file := range oldPool {
		file.Close()
	}

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
		b.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				_, value := benchtop.ParsePosKey(it.Key())
				out <- benchtop.Index{Key: value}
			}
			return nil
		})
	}()
	return out, nil
}

func (b *BSONTable) Scan(keys bool, filter []benchtop.FieldFilter, fields ...string) (chan map[string]any, error) {
	b.handleLock.RLock()
	defer b.handleLock.RUnlock()

	out := make(chan map[string]any, 100)
	_, err := b.handle.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(out)
		var offsetSizeData [8]byte
		var sizeBytes [4]byte
		rowData := make([]byte, 0)
		fmt.Println("ENTERING SCAN++++++++++++++++++++++++++++++++++++++")
		for {
			_, err := b.handle.Read(offsetSizeData[:])
			if err != nil {
				if err == io.EOF {
					break
				}
				return
			}
			nextOffset := binary.LittleEndian.Uint64(offsetSizeData[:])
			_, err = b.handle.Read(sizeBytes[:])
			if err != nil {
				return
			}
			bSize := int32(binary.LittleEndian.Uint32(sizeBytes[:]))

			// Elem has been deleted or at the table header in the begginning of the file skip it.
			if bSize == 0 || int64(bSize) == int64(nextOffset)-8 {
				_, err = b.handle.Seek(int64(nextOffset), io.SeekStart)
				if err == io.EOF {
					break
				}
				continue
			}
			if cap(rowData) < int(bSize) {
				rowData = make([]byte, bSize)
			} else {
				rowData = rowData[:bSize]
			}
			copy(rowData, sizeBytes[:])

			_, err = b.handle.Read(rowData[4:])
			if err != nil {
				return
			}

			bd, ok := bson.Raw(rowData).Lookup("R").ArrayOK()
			if !ok {
				return
			}
			columns := bd.Index(0).Value().Array()

			var key string
			if keys {
				key = bd.Index(2).Value().StringValue()
			}

			rowMap := make(map[string]any)

            // Unpack named columns
            for i, c := range b.columns {
                unpack, err := b.colUnpack(columns.Index(uint(i)), c.Type)
                if err != nil {
                    continue // Skip invalid column data
                }
                rowMap[c.Key] = unpack
            }

            // Unpack 'other data'
            var otherMap map[string]any
            err = bson.Unmarshal(bd.Index(1).Value().Document(), &otherMap)
            if err != nil {
                continue // Skip if 'other data' cannot be unmarshaled
            }
            for k, v := range otherMap {
                rowMap[k] = v
            }

            // Add key to rowMap if requested
            if keys {
                rowMap["_key"] = key
            }

            // Step 2: Apply filters to the entire row
            if len(filter) == 0 || filters.PassesFilters(rowMap, filter) {
                // Step 3: Construct output based on fields
                vOut := make(map[string]any)
                if len(fields) == 0 {
                    // Include all fields when fields is empty
                    for k, v := range rowMap {
                        vOut[k] = v
                    }
                } else {
                    // Include only specified fields
                    for _, colName := range fields {
                        if val, ok := rowMap[colName]; ok {
                            vOut[colName] = val
                        }
                    }
                    if keys && vOut["_key"] == nil { // Ensure key is included if requested
                        vOut["_key"] = key
                    }
                }
                if len(vOut) > 0 {
                	fmt.Println("PASSING VOUT+++++++++")
                    out <- vOut
                }
            }

			_, err = b.handle.Seek(int64(nextOffset), io.SeekStart)
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
					results <- benchtop.BulkResponse{Key: index.Key, Data: nil, Err: func() string {
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

				results <- benchtop.BulkResponse{Key: index.Key, Data: data, Err: func() string {
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
			mData, err := b.packData(entry.Data, string(entry.Id))
			if err != nil {
				errs = multierror.Append(errs, err)
				log.Errorf("pack data err in Load: bulkSet: %s", err)
			}
			bData, err := bson.Marshal(mData)
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
			b.addTableDeleteEntryInfo(tx, entry.Id, entry.TableName)
			b.addTableEntryInfo(tx, entry.Id, uint64(offset), uint64(writeSize))
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
				results <- benchtop.BulkResponse{Key: index.Key, Data: nil, Err: func() string {
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

				val, closer, err := b.db.Get(benchtop.NewPosKey(b.tableId, index.Key))
				if err != nil {
					results <- benchtop.BulkResponse{Key: index.Key, Data: nil, Err: func() string {
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
					results <- benchtop.BulkResponse{Key: index.Key, Data: nil, Err: func() string {
						if err != nil {
							return err.Error()
						}
						return ""
					}()}
					return
				}

				batchDeletes <- index
				results <- benchtop.BulkResponse{Key: index.Key, Data: nil, Err: ""}
			}(index)
		}
		wg.Wait()
	}()

	return results
}
