package jsontable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
	"github.com/edsrzf/mmap-go"
	multierror "github.com/hashicorp/go-multierror"

	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
)

type JSONTable struct {
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

func (b *JSONTable) Init(poolSize int) error {
	b.FilePool = make(chan *os.File, poolSize)
	for i := range poolSize {
		file, err := os.OpenFile(b.Path, os.O_RDWR, 0666)
		if err != nil {
			for range i {
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

func (b *JSONTable) GetColumnDefs() []benchtop.ColumnDef {
	return b.columns
}

func (b *JSONTable) Close() {
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
func (b *JSONTable) AddRow(elem benchtop.Row) (*benchtop.RowLoc, error) {

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

	//log.Debugln("WRITE ENTRY: ", offset, len(bData))
	writesize, err := b.writeJsonEntry(offset, bData)
	if err != nil {
		log.Errorf("write handler err in Load: bulkSet: %s", err)
		return nil, err
	}

	return &benchtop.RowLoc{
		Offset: uint64(offset),
		Size:   uint64(writesize),
		Label:  b.TableId,
	}, nil
}

func (b *JSONTable) GetRow(loc benchtop.RowLoc) (map[string]any, error) {

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
	out, err := b.unpackData(true, false, &m)
	if err != nil {
		return nil, err
	}
	return out.(map[string]any), nil
}

func (b *JSONTable) MarkDeleteTable(loc benchtop.RowLoc) error {
	// Since we're not explicitly 'adding' to a part of the file, should be able
	// to get away with no lock here since the space is just 'marked' as empty
	file := <-b.FilePool
	defer func() {
		b.FilePool <- file
	}()
	if _, err := file.WriteAt([]byte{0x00, 0x00, 0x00, 0x00}, int64(loc.Offset+ROW_OFFSET_HSIZE)); err != nil {
		return fmt.Errorf("writeAt failed: %w", err)
	}
	return nil
}

func (b *JSONTable) DeleteRow(loc benchtop.RowLoc, id []byte) error {
	b.handleLock.Lock()
	defer b.handleLock.Unlock()

	if _, err := b.handle.WriteAt([]byte{0x00, 0x00, 0x00, 0x00}, int64(loc.Offset+ROW_OFFSET_HSIZE)); err != nil {
		return fmt.Errorf("writeAt failed: %w", err)
	}
	err := b.db.Delete(benchtop.NewPosKey(b.TableId, id), nil)
	if err != nil {
		return err
	}
	return nil
}

/*
////////////////////////////////////////////////////////////////
Start of bulk, chan based functions
*/
func (b *JSONTable) Keys() (chan benchtop.Index, error) {
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

func (b *JSONTable) Scan(loadData bool, filter benchtop.RowFilter) chan any {
	outChan := make(chan any, 100)
	go func() {
		defer close(outChan)
		handle := <-b.FilePool
		_, err := handle.Seek(0, io.SeekStart)
		if err != nil {
			log.Errorln("Error in jsontable scan func", err)
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
		for offset+ROW_HSIZE <= len(m) {
			header := m[offset : offset+ROW_HSIZE]
			nextOffset := binary.LittleEndian.Uint64(header[:ROW_OFFSET_HSIZE])
			bSize := int32(binary.LittleEndian.Uint32(header[ROW_OFFSET_HSIZE:ROW_HSIZE]))

			if bSize == 0 || int64(bSize) == int64(nextOffset)-ROW_HSIZE {
				offset = int(nextOffset)
				continue
			}

			jsonStart := offset + ROW_HSIZE
			jsonEnd := jsonStart + int(bSize)
			if jsonEnd > len(m) {
				log.Debugf("Incomplete record at end of file at offset %d", offset)
				break
			}

			rowData := m[jsonStart:jsonEnd]
			err = b.processJSONRowData(rowData, loadData, filter, outChan)
			if err != nil {
				log.Debugf("Skipping malformed row at offset %d: %v", offset, err)
			}
			offset = int(nextOffset)

		}
	}()
	return outChan
}

// processBSONRowData handles the parsing of row bytes,
// applying filters, and sending the result to the output channel.
// It returns an error if the row is malformed or cannot be processed.
func (b *JSONTable) processJSONRowData(
	rowData []byte,
	loadData bool,
	filter benchtop.RowFilter,
	outChan chan any,
) error {
	var val any
	var err error

	if loadData || filter != nil && !filter.IsNoOp() {
		var m RowData
		sonic.ConfigFastest.Unmarshal(rowData, &m)
		val, err = b.unpackData(true, true, &m)
		if err != nil {
			return err
		}
	} else {
		val = rowData
	}

	if filter == nil || filter.IsNoOp() || (!filter.IsNoOp() && filter.Matches(val)) {
		if loadData {
			outChan <- val
			return nil
		}

		node, err := sonic.Get(rowData, "1")
		if err != nil {
			log.Errorf("Error accessing JSON path for row data %s: %v\n", string(rowData), err)
			return err
		}
		ID, err := node.Interface()
		if err != nil {
			log.Errorf("Error unmarshaling node: %v\n", err)
			return err
		}
		outChan <- ID
	}
	return nil
}

// Compact, Fetch, Load, And Remove methods are not currently being used in grip.
// Compact should be introduced into grip in a future PR since the heavy load and delete design approach that we are taking
func (b *JSONTable) Compact() error {
	const flushThreshold = 1000
	flushCounter := 0
	b.handleLock.Lock()
	defer b.handleLock.Unlock()

	tempFileName, err := filepath.Abs(b.handle.Name() + ".compact")
	if err != nil {
		return fmt.Errorf("failed to get absolute path for temp file: %w", err)
	}

	tempHandle, err := os.Create(tempFileName)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempHandle.Close()

	oldHandle := b.handle
	m, err := mmap.Map(oldHandle, mmap.RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to map file: %w", err)
	}
	defer m.Unmap()

	writer := bufio.NewWriterSize(tempHandle, 16*1024*1024)
	var newOffset uint64 = 0
	inputChan := make(chan benchtop.Index, 100)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.setDataIndices(inputChan)
	}()

	offset := 0
	for offset+ROW_HSIZE <= len(m) {
		header := m[offset : offset+ROW_HSIZE]
		nextOffset := binary.LittleEndian.Uint64(header[:ROW_OFFSET_HSIZE])
		bSize := int32(binary.LittleEndian.Uint32(header[ROW_OFFSET_HSIZE:ROW_HSIZE]))

		if bSize == 0 || int64(nextOffset) == int64(12) {
			if int64(nextOffset) > int64(offset) {
				offset = int(nextOffset)
			}
			continue
		}

		jsonStart := offset + 12
		jsonEnd := jsonStart + int(bSize)
		if jsonEnd > len(m) {
			return fmt.Errorf("incomplete JSON data at offset %d, size %d", offset, bSize)
		}

		rowData := m[jsonStart:jsonEnd]
		var mRow RowData
		err = sonic.ConfigFastest.Unmarshal(rowData, &mRow)
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("JSON data for row at offset %d, size %d was incomplete: %w", offset, bSize, err)
			}
			return fmt.Errorf("failed to decode JSON row at offset %d, size %d: %w", offset, bSize, err)
		}

		node, err := sonic.Get(rowData, "1")
		if err != nil {
			return fmt.Errorf("failed to access ID field for row at offset %d: %w", offset, err)
		}
		key, err := node.String()
		if err != nil {
			return fmt.Errorf("failed to unmarshal ID field for row at offset %d: %w", offset, err)
		}
		inputChan <- benchtop.Index{Key: []byte(key), Position: newOffset, Size: uint64(bSize)}

		newOffsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(newOffsetBytes, newOffset+uint64(bSize)+12)

		_, err = writer.Write(newOffsetBytes)
		if err != nil {
			return fmt.Errorf("failed writing new offset at %d: %w", newOffset, err)
		}
		_, err = writer.Write(rowData)
		if err != nil {
			return fmt.Errorf("failed writing JSON row at offset %d: %w", newOffset, err)
		}

		flushCounter++
		if flushCounter%flushThreshold == 0 {
			if err := writer.Flush(); err != nil {
				return fmt.Errorf("failed flushing writer: %w", err)
			}
		}

		newOffset += uint64(bSize) + 8
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
		return fmt.Errorf("failed to get absolute path for file: %w", err)
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
	for range oldPool {
		file, err := os.Open(b.Path)
		if err != nil {
			return fmt.Errorf("failed to refresh file pool: %w", err)
		}
		b.FilePool <- file
	}
	close(oldPool)
	for file := range oldPool {
		file.Close()
	}

	return nil
}

func (b *JSONTable) Fetch(inputs chan benchtop.Index, workers int) <-chan benchtop.BulkResponse {
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

func (b *JSONTable) Load(inputs chan benchtop.Row) error {
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
				log.Errorf("json Marshall err in Load: bulkSet: %s", err)
			}

			// make Next offset equal to existing offset + length of data
			writeSize, err := b.writeJsonEntry(offset, bData)
			if err != nil {
				errs = multierror.Append(errs, err)
				log.Errorf("write handler err in Load: bulkSet: %s", err)
			}
			b.AddTableEntryInfo(tx, entry.Id, benchtop.RowLoc{Offset: uint64(offset), Size: uint64(writeSize)})
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

func (b *JSONTable) Remove(inputs chan benchtop.Index, workers int) <-chan benchtop.BulkResponse {
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

func ConvertJSONPathToArray(path string) ([]any, error) {
	path = strings.TrimLeft(path, "./")
	result := []any{"0"}

	re := regexp.MustCompile(`[^.\[\]]+|\[\d+\]`)
	matches := re.FindAllString(path, -1)
	for _, token := range matches {
		if strings.HasPrefix(token, "[") && strings.HasSuffix(token, "]") {
			numStr := token[1 : len(token)-1]
			index, err := strconv.Atoi(numStr)
			if err != nil {
				return nil, fmt.Errorf("invalid array index: %s", token)
			}
			result = append(result, index)
		} else {
			result = append(result, token)
		}
	}
	return result, nil
}
