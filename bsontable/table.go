package bsontable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
	multierror "github.com/hashicorp/go-multierror"

	"github.com/cockroachdb/pebble"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

func (b *BSONTable) Scan(keys bool, filter benchtop.RowFilter, fields ...string) chan any {
	handle, ok := <-b.filePool
	if !ok {
		log.Errorln("Error: File pool is closed.")
		outChan := make(chan any)
		close(outChan)
		return outChan
	}

	// Create a single channel of type chan any
	outChan := make(chan any, 100)

	_, err := handle.Seek(0, io.SeekStart)
	if err != nil {
		close(outChan) // Close the channel if an error occurs before the goroutine starts
		log.Errorln("Error in bsontable scan func", err)
		return nil
	}

	var filterFields []string
	if filter != nil {
		filterFields = filter.RequiredFields()
	}
	allFields := len(fields) == 0
	selectedFields := fields
	if allFields && !keys {
		selectedFields = make([]string, len(b.columns))
		for i, col := range b.columns {
			selectedFields[i] = col.Key
		}
	}
	requiredFields := union(filterFields, selectedFields)

	go func() {
		defer func() {
			close(outChan)
			b.filePool <- handle // Return handle to pool
		}()

		const bufferSize = 16 << 20 // 1MB buffer
		buffer := make([]byte, bufferSize)
		var bufStart, bufEnd, filePos int64
		var leftover []byte

		for {
			// Fill buffer if empty or insufficient data
			if bufEnd-bufStart < 12 || (len(leftover) > 0 && int64(len(leftover)) < bufEnd-bufStart) {
				// Shift remaining data to start
				if bufStart < bufEnd {
					copy(buffer[:bufEnd-bufStart], buffer[bufStart:bufEnd])
				}
				bufEnd -= bufStart
				bufStart = 0

				// Read more data
				n, err := handle.Read(buffer[bufEnd:])
				if err == io.EOF && bufEnd == 0 && len(leftover) == 0 {
					break
				}
				if err != nil && err != io.EOF {
					log.Errorln("Error reading file", err)
					return
				}
				bufEnd += int64(n)
				filePos += int64(n)
			}

			// Combine leftover with current buffer
			data := buffer[bufStart:bufEnd]
			if len(leftover) > 0 {
				data = append(leftover, data...)
				leftover = nil
			}

			// Process records in buffer
			for pos := int64(0); pos+12 <= int64(len(data)); {
				nextOffset := binary.LittleEndian.Uint64(data[pos : pos+8])
				bSize := int32(binary.LittleEndian.Uint32(data[pos+8 : pos+12]))

				// Skip invalid records
				if bSize == 0 || int64(bSize) == int64(nextOffset)-8 {
					if nextOffset < uint64(filePos-bufEnd+bufStart+pos) {
						log.Errorln("Invalid nextOffset, seeking backward")
						return
					}
					if nextOffset > uint64(filePos) {
						// Seek to next record
						_, err := handle.Seek(int64(nextOffset), io.SeekStart)
						if err != nil {
							log.Errorln("Error seeking", err)
							return
						}
						filePos = int64(nextOffset)
						bufStart, bufEnd = 0, 0
						leftover = nil
						break
					}
					pos = int64(nextOffset) - (filePos - bufEnd)
					continue
				}

				// Check if entire record is in buffer
				if pos+8+int64(bSize) > int64(len(data)) {
					leftover = data[pos:]
					bufStart = bufEnd
					break
				}

				// Extract row data
				rowData := data[pos+8 : pos+8+int64(bSize)]

				// Parse BSON row
				bd, ok := bson.Raw(rowData).Lookup("R").ArrayOK()
				if !ok {
					pos += 8 + int64(bSize)
					continue
				}
				columns := bd.Index(0).Value().Array()
				key := bd.Index(2).Value().StringValue()

				// Build row map
				rowMap := make(map[string]any, len(requiredFields))
				for i, col := range b.columns {
					if allFields || slices.Contains(requiredFields, col.Key) {
						if unpack, err := b.colUnpack(columns.Index(uint(i)), col.Type); err == nil {
							rowMap[col.Key] = unpack
						}
					}
				}

				otherData := bd.Index(1).Value().Document()
				if allFields {
					var otherMap map[string]any
					if err := bson.Unmarshal(otherData, &otherMap); err == nil {
						for k, v := range otherMap {
							rowMap[k] = convertBSONValue(v)
						}
					}
				} else {
					for _, field := range requiredFields {
						if !isNamedColumn(field, b.columns) {
							if val, err := otherData.LookupErr(field); err == nil {
								rowMap[field] = convertBSONValue(val)
							}
						}
					}
				}

				// Apply filter and send output
				if filter == nil || filter.Matches(rowMap) {
					if keys {
						outChan <- key
					} else {
						vOut := make(map[string]any)
						if allFields {
							maps.Copy(vOut, rowMap)
							vOut["_key"] = key
							vOut["_id"] = key
						} else {
							for _, colName := range selectedFields {
								if val, ok := rowMap[colName]; ok {
									vOut[colName] = val
								}
							}
						}
						if len(vOut) > 0 {
							outChan <- vOut
						}
					}
				}

				// Move to next record
				pos += 8 + int64(bSize)
			}

			// Update buffer position
			if len(leftover) == 0 {
				bufStart += int64(len(data))
			}
		}
	}()

	return outChan
}

func convertBSONValue(val any) any {
	switch v := val.(type) {
	case primitive.D: // Ordered BSON document
		m := make(map[string]any)
		for _, elem := range v {
			m[elem.Key] = convertBSONValue(elem.Value) // Recurse for nested values
		}
		return m
	case primitive.M: // Unordered BSON document
		m := make(map[string]any)
		for key, value := range v {
			m[key] = convertBSONValue(value) // Recurse for nested values
		}
		return m
	case primitive.A: // BSON array
		arr := make([]any, len(v))
		for i, elem := range v {
			arr[i] = convertBSONValue(elem) // Recurse for array elements
		}
		return arr
	case primitive.ObjectID: // Convert ObjectID to its string representation
		return v.Hex()
	case primitive.DateTime: // Convert BSON DateTime to Go's time.Time
		return v.Time()
	// Add other specific primitive types if you need custom conversions, e.g.,
	// case primitive.Decimal128:
	//    return v.String() // Convert Decimal128 to string
	default:
		// For all other types (string, int, float, bool, nil, etc.), return as is
		return val
	}
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

func union(a, b []string) []string {
	set := make(map[string]struct{})
	for _, v := range a {
		set[v] = struct{}{}
	}
	for _, v := range b {
		set[v] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for k := range set {
		result = append(result, k)
	}
	return result
}

func isNamedColumn(field string, columns []benchtop.ColumnDef) bool {
	for _, col := range columns {
		if col.Key == field {
			return true
		}
	}
	return false
}
