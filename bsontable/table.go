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
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
	"github.com/edsrzf/mmap-go"
	multierror "github.com/hashicorp/go-multierror"

	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"

	"go.mongodb.org/mongo-driver/bson"
)

type BSONTable struct {
	Pb        *pebblebulk.PebbleKV
	db        *pebble.DB
	columns   []benchtop.ColumnDef
	columnMap map[string]int

	FilePool   chan *os.File
	handle     *os.File
	handleLock sync.RWMutex
	TableId    uint16

	Path     string
	Name     string
	FileName string
}

func (b *BSONTable) Init(poolSize int) error {
	b.FilePool = make(chan *os.File, poolSize)
	for i := 0; i < poolSize; i++ {
		file, err := os.Open(b.Path)
		if err != nil {
			// Close already opened files
			for j := 0; j < i; j++ {
				if file, ok := <-b.FilePool; ok {
					file.Close()
				}
			}
			return fmt.Errorf("failed to init file pool for %s: %v", b.Path, err)
		}
		b.FilePool <- file
	}
	return nil
}

func (b *BSONTable) GetColumnDefs() []benchtop.ColumnDef {
	return b.columns
}

func (b *BSONTable) Close() {
	if b.FilePool != nil {
		for len(b.FilePool) > 0 {
			if file, ok := <-b.FilePool; ok {
				file.Close()
			}
		}
		close(b.FilePool)
	}
	//because the table could be opened by other threads, don't actually close
}

/*
////////////////////////////////////////////////////////////////
Unary single effect operations
*/
func (b *BSONTable) AddRow(elem benchtop.Row) (*benchtop.RowLoc, error) {

	bData, err := sonic.ConfigFastest.Marshal(
		b.packData(elem.Data, string(elem.Id)),
	)
	if err != nil {
		return nil, err
	}

	//append to end of block file
	b.handleLock.Lock()
	defer b.handleLock.Unlock()
	offset, err := b.handle.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	log.Debugln("WRITE ENTRY: ", offset, len(bData))
	writesize, err := b.writeBsonEntry(offset, bData)
	if err != nil {
		log.Errorf("write handler err in Load: bulkSet: %s", err)
		return nil, err
	}

	return &benchtop.RowLoc{
		Offset: 	uint64(offset),
		Size:   	uint64(writesize),
		Label:   	b.TableId,
	}, nil
}

func (b *BSONTable) GetRow(loc benchtop.RowLoc) (map[string]any, error) {
	file := <-b.FilePool
	defer func() {
		b.FilePool <- file
	}()

	// Offset skip the first 8 bytes since they are for getting the offset for a scan operation
	_, err := file.Seek(int64(loc.Offset+12), io.SeekStart)
	if err != nil {
		return nil, err
	}

	decoder := sonic.ConfigFastest.NewDecoder(io.LimitReader(file, int64(loc.Size)))
	var m RowData
	err = decoder.Decode(&m)
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("JSON data for row at offset %d, size %d was incomplete: %w", loc.Offset, loc.Size, err)
		}
		return nil, fmt.Errorf("failed to decode JSON row at offset %d, size %d: %w", loc.Offset, loc.Size, err)
	}
	out, err := b.unpackData(false, false, &m)
	if err != nil {
		return nil, err
	}
	return out.(map[string]any), nil
}

func (b *BSONTable) DeleteRow(name []byte) error {
	offset, _, err := b.GetBlockPos(name)
	if err != nil {
		return err
	}
	b.handleLock.Lock()
	if _, err := b.handle.WriteAt([]byte{0x00, 0x00, 0x00, 0x00}, int64(offset+12)); err != nil {
		return fmt.Errorf("writeAt failed: %w", err)
	}
	b.handleLock.Unlock()
	b.db.Delete(benchtop.NewPosKey(b.TableId, name), nil)
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

		fileOffset += 12
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
		binary.LittleEndian.PutUint64(newOffsetBytes, newOffset+uint64(len(rowBuff))+12)

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

	oldPool := b.FilePool
	b.FilePool = make(chan *os.File, cap(oldPool))
	for i := 0; i < cap(oldPool); i++ {
		file, err := os.Open(b.Path)
		if err != nil {
			return fmt.Errorf("failed to refresh file pool: %v", err)
		}
		b.FilePool <- file
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
		prefix := benchtop.NewPosKeyPrefix(b.TableId)
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
	outChan := make(chan any, 100)
	go func() {
		defer close(outChan)
		handle := <-b.FilePool
		_, err := handle.Seek(0, io.SeekStart)
		if err != nil {
			log.Errorln("Error in bsontable scan func", err)
			return
		}
		
		m, err := mmap.Map(handle, mmap.RDONLY, 0)
		if err != nil {
			log.Errorln("Error mapping file:", err)
			return
		}
		
		defer func() {
			b.FilePool <- handle
			defer m.Unmap()
		}()


		// Process the memory-mapped data
		offset := 0
		for offset+ ROW_HSIZE <= len(m) {
			header := m[offset : offset+ ROW_HSIZE]
			nextOffset := binary.LittleEndian.Uint64(header[:ROW_OFFSET_HSIZE])
			bSize := int32(binary.LittleEndian.Uint32(header[ROW_OFFSET_HSIZE:ROW_HSIZE]))

			if bSize == 0 || int64(bSize) == int64(nextOffset)- ROW_HSIZE {
				offset = int(nextOffset)
				continue
			}

			bsonStart := offset + ROW_HSIZE
			bsonEnd := bsonStart + int(bSize)
			if bsonEnd > len(m) {
				log.Debugf("Incomplete record at end of file at offset %d", offset)
				break
			}

			rowData := m[bsonStart:bsonEnd]

			err = b.processBSONRowData(rowData, keys, filter, outChan)
			if err != nil {
				log.Debugf("Skipping malformed row at offset %d: %v", offset, err)
			}
			offset = int(nextOffset)

		}
	}()
	return outChan
}

// processBSONRowData handles the parsing of a raw BSON row,
// applying filters, and sending the result to the output channel.
// It returns an error if the BSON is malformed or cannot be processed.
func (b *BSONTable) processBSONRowData(
	rowData []byte,
	keys bool,
	filter benchtop.RowFilter,
	outChan chan any,
) error {

	var m RowData
	sonic.ConfigFastest.Unmarshal(rowData, &m)
	res, err := b.unpackData(false, true, &m)
	if err != nil {
		return err
	}

	if filter == nil || filter.IsNoOp() || !filter.IsNoOp() && filter.Matches(res.(map[string]any)) {
		if keys {
			outChan <- res.(map[string]any)["_id"]
		} else {
			outChan <- res
		}
	}
	return nil
}


func (b *BSONTable) Fetch(inputs chan benchtop.Index, workers int) <-chan benchtop.BulkResponse {
	results := make(chan benchtop.BulkResponse, workers)
	var wg sync.WaitGroup
	go func() {
		for entry := range inputs {
			wg.Add(1)
			go func(index benchtop.Index) {
				defer wg.Done()
				val, closer, err := b.db.Get(benchtop.NewPosKey(b.TableId, index.Key))
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

			bData, err := sonic.Marshal(
				b.packData(entry.Data, string(entry.Id)),
			)
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
			b.AddTableEntryInfo(tx, entry.Id, benchtop.RowLoc{Offset:uint64(offset), Size:uint64(writeSize)})
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
			err := b.db.Delete(benchtop.NewPosKey(b.TableId, index.Key), nil)
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

				val, closer, err := b.db.Get(benchtop.NewPosKey(b.TableId, index.Key))
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
