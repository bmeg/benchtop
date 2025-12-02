package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/DataDog/zstd"
	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/section"
	"github.com/bmeg/grip/log"
	"github.com/edsrzf/mmap-go"

	"github.com/bytedance/sonic"
)

const (
	PART_FILE_SUFFIX    string = ".partition"
	SECTION_FILE_SUFFIX string = ".section"
	SECTION_ID_MULT     uint16 = 256
	MAX_COMPACT_RATIO          = 0.2 // 20% deleted rows triggers compaction
	FLUSH_THRESHOLD            = 1000
)

type JSONTable struct {
	// Artifact arguments
	Columns   []benchtop.ColumnDef
	ColumnMap map[string]int

	TableId  uint16
	Path     string // Base path (for legacy single file)
	Name     string
	FileName string // Base name for section files

	Fields map[string]struct{} // Indexing moved to table level

	Sections              map[uint16]*section.Section // sectionId -> Section
	PartitionMap          map[uint8][]uint16          // partitionId -> []sectionId
	SectionLock           sync.Mutex                  // For creating new sections
	NumPartitions         uint32                      // Number of partitions
	PartitionFunc         func(id []byte) uint8       // Assigns row to partition
	MaxConcurrentSections uint8                       // Limit for parallel operations

	ActiveSections map[uint8]*section.Section // one per partition
	FlushCounter   map[uint8]int              // per-partition flush counter
}

// DefaultPartitionFunc assigns rows to partitions using FNV hash
func defaultPartitionFunc(numPartitions uint32) func(id []byte) uint8 {
	return func(id []byte) uint8 {
		h := fnv.New32a()
		h.Write(id)
		return uint8(h.Sum32() % numPartitions)
	}
}

func (b *JSONTable) Close() error {
	for _, sec := range b.Sections {
		if sec.MMap != nil {
			err := sec.MMap.Unmap()
			if err != nil {
				fmt.Printf("ERROR ON UNMAP: %s", err)
				return err
			}
		}
		if sec.File != nil {
			err := sec.File.Sync()
			if err != nil {
				fmt.Printf("ERROR ON FILE HANDLE SYNC: %s", err)
				return err
			}
			err = sec.File.Close()
			if err != nil {
				fmt.Printf("ERROR ON FILE HANDLE CLOSE: %s", err)
				return err
			}
		}
		if sec.FilePool != nil {
			close(sec.FilePool)
			for f := range sec.FilePool {
				err := f.Close()
				if err != nil {
					fmt.Printf("ERROR ON FILE POOL FILE HANDLE CLOSE: %s", err)
					return err
				}
			}
		}
	}
	b.Fields = map[string]struct{}{}
	return nil
}

// AddRow adds a single row to the JSONTable, writing it as zstd-compressed data.
func (b *JSONTable) AddRow(elem benchtop.Row) (*benchtop.RowLoc, error) {
	partitionId := b.PartitionFunc(elem.Id)
	if partitionId >= uint8(b.NumPartitions) {
		return nil, fmt.Errorf("invalid partition")
	}

	// Get or create active section
	sec := b.ActiveSections[partitionId]
	if sec == nil {
		var err error
		sec, err = b.CreateNewSection(partitionId)
		if err != nil {
			return nil, err
		}
	}

	bData, err := sonic.ConfigFastest.Marshal(b.PackData(elem.Data, string(elem.Id)))
	if err != nil {
		return nil, err
	}
	totalSize := uint32(len(bData)) + benchtop.ROW_HSIZE

	// Check size and rotate if needed
	if sec.LiveBytes+totalSize > section.MAX_SECTION_SIZE {
		// Close current
		err := sec.CloseSection()
		if err != nil {
			sec.Lock.Unlock()
			return nil, err
		}
		// Create new active
		newSec, err := b.CreateNewSection(partitionId)
		if err != nil {
			return nil, err
		}
		sec = newSec
		b.ActiveSections[partitionId] = sec
	}

	loc, err := sec.WriteJsonEntryToSection(bData)
	if err != nil {
		return nil, err
	}

	sec.TotalRows++
	loc.TableId = b.TableId
	return loc, nil
}

func (b *JSONTable) CreateNewSection(partitionId uint8) (*section.Section, error) {
	b.SectionLock.Lock()
	defer b.SectionLock.Unlock()

	localSecId := len(b.PartitionMap[partitionId])
	secId := uint16(partitionId)*SECTION_ID_MULT + uint16(localSecId)
	if _, exists := b.Sections[secId]; exists {
		return nil, fmt.Errorf("section ID conflict: %d", secId)
	}

	path := fmt.Sprintf("%s%s%d.section%d", b.FileName, PART_FILE_SUFFIX, partitionId, localSecId)
	handle, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	handle.Truncate(section.INITIAL_SECTION_SIZE) // pre-allocate

	m, err := mmap.Map(handle, mmap.RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("mmap failed on new section: %w", err)
	}

	filePool := make(chan *os.File, 10)
	for range cap(filePool) {
		f, err := os.OpenFile(path, os.O_RDWR, 0666)
		if err != nil {
			m.Unmap()
			handle.Close()
			return nil, err
		}
		filePool <- f
	}

	sec := &section.Section{
		ID:              secId,
		PartitionID:     partitionId,
		Path:            path,
		File:            handle,
		FilePool:        filePool,
		MMap:            m,
		MMapMode:        mmap.RDWR,
		Active:          true,
		LiveBytes:       0,
		CompressScratch: make([]byte, 0),
	}

	b.Sections[secId] = sec
	b.PartitionMap[partitionId] = append(b.PartitionMap[partitionId], secId)
	b.ActiveSections[partitionId] = sec
	b.FlushCounter[partitionId] = 0
	return sec, nil
}

func (b *JSONTable) GetRow(loc *benchtop.RowLoc) (map[string]any, error) {
	sec, exists := b.Sections[loc.Section]
	if !exists {
		return nil, fmt.Errorf("section %d not found", loc.Section)
	}

	if len(sec.MMap) == 0 {
		return nil, fmt.Errorf("section %d is empty or not mapped", loc.Section)
	}

	start := loc.Offset + benchtop.ROW_HSIZE
	end := start + loc.Size
	if end > uint32(len(sec.MMap)) {
		return nil, fmt.Errorf("row out of bounds: %d > %d", end, len(sec.MMap))
	}

	compressed := sec.MMap[start:end]
	decompressed, err := zstd.Decompress(nil, compressed)
	if err != nil {
		return nil, fmt.Errorf("decompress failed: %w", err)
	}

	var m RowData
	if err := sonic.ConfigFastest.Unmarshal(decompressed, &m); err != nil {
		return nil, fmt.Errorf("unmarshal failed: %w", err)
	}

	return m.Data, nil
}

func (b *JSONTable) MarkDeleteTable(loc *benchtop.RowLoc) error {
	sec, exists := b.Sections[loc.Section]
	if !exists {
		return fmt.Errorf("section %d not found", loc.Section)
	}

	file := <-sec.FilePool
	defer func() { sec.FilePool <- file }()

	_, err := file.WriteAt(bytes.Repeat([]byte{0x00}, 4), int64(loc.Offset+benchtop.ROW_OFFSET_HSIZE))
	if err != nil {
		return fmt.Errorf("writeAt failed: %w", err)
	}
	sec.Lock.Lock()
	sec.DeletedRows++
	sec.LiveBytes -= loc.Size
	sec.Lock.Unlock()
	return nil
}

func (b *JSONTable) DeleteRow(loc *benchtop.RowLoc, id []byte) error {
	sec, exists := b.Sections[loc.Section]
	if !exists {
		return fmt.Errorf("section %d not found", loc.Section)
	}

	sec.Lock.Lock()
	defer sec.Lock.Unlock()

	_, err := sec.File.Seek(int64(loc.Offset+benchtop.ROW_OFFSET_HSIZE), io.SeekStart)
	if err != nil {
		return err
	}
	_, err = sec.File.Write(bytes.Repeat([]byte{0x00}, 4))
	if err != nil {
		return fmt.Errorf("writeAt failed: %w", err)
	}
	sec.DeletedRows++
	sec.LiveBytes -= loc.Size
	return nil
}

func (b *JSONTable) ScanDoc(filter benchtop.RowFilter) chan map[string]any {
	outChan := make(chan map[string]any, 100*len(b.Sections))
	var wg sync.WaitGroup
	sem := make(chan struct{}, b.MaxConcurrentSections)
	for pId := uint8(0); pId < uint8(b.NumPartitions); pId++ {
		for _, secId := range b.PartitionMap[pId] {
			sec, exists := b.Sections[secId]
			if !exists || len(sec.MMap) == 0 {
				continue
			}
			wg.Add(1)
			go func(sec *section.Section) {
				sem <- struct{}{}
				defer func() { <-sem; wg.Done() }()
				m := sec.MMap
				var offset uint32 = 0
				for offset+benchtop.ROW_HSIZE <= uint32(len(m)) {
					header := m[offset : offset+benchtop.ROW_HSIZE]
					nextOffset := binary.LittleEndian.Uint32(header[:benchtop.ROW_OFFSET_HSIZE])
					bSize := binary.LittleEndian.Uint32(header[benchtop.ROW_OFFSET_HSIZE:benchtop.ROW_HSIZE])
					if bSize == 0 {
						if nextOffset == 0 || nextOffset <= offset {
							break
						}
						offset = nextOffset
						continue
					}
					jsonStart := offset + benchtop.ROW_HSIZE
					jsonEnd := jsonStart + bSize
					if jsonEnd > uint32(len(m)) {
						break
					}
					rowData := m[jsonStart:jsonEnd]
					if err := b.processJSONRowDataDoc(rowData, filter, outChan); err != nil {
						log.Debugf("skip row in section %d: %v", sec.ID, err)
					}
					if nextOffset == 0 || nextOffset <= offset {
						break
					}
					offset = nextOffset
				}
			}(sec)
		}
	}
	go func() { wg.Wait(); close(outChan) }()
	return outChan
}

// processJSONRowDataDoc handles parsing of row bytes for ScanDoc, applying filters, and sending RowData to the output channel.
func (b *JSONTable) processJSONRowDataDoc(rowData []byte, filter benchtop.RowFilter, outChan chan map[string]any) error {
	newData, err := zstd.Decompress(nil, rowData)
	if err != nil {
		return err
	}
	if filter != nil && !filter.IsNoOp() {
		if !filter.Matches(newData, b.Name) {
			return nil
		}
	}
	var m RowData
	err = sonic.ConfigFastest.Unmarshal(newData, &m)
	if err != nil {
		return err
	}
	if m.Data != nil {
		m.Data["_id"] = m.Key
	}
	outChan <- m.Data
	return nil
}

// ScanId scans the JSONTable and returns IDs (as string) that match the filter.
func (b *JSONTable) ScanId(filter benchtop.RowFilter) chan string {
	outChan := make(chan string, 100*len(b.Sections))
	var wg sync.WaitGroup
	sem := make(chan struct{}, b.MaxConcurrentSections)
	for pId := uint8(0); pId < uint8(b.NumPartitions); pId++ {
		for _, secId := range b.PartitionMap[pId] {
			sec, exists := b.Sections[secId]
			if !exists || len(sec.MMap) == 0 {
				continue
			}
			wg.Add(1)
			go func(sec *section.Section) {
				sem <- struct{}{}
				defer func() { <-sem; wg.Done() }()
				m := sec.MMap
				var offset uint32 = 0
				for offset+benchtop.ROW_HSIZE <= uint32(len(m)) {
					header := m[offset : offset+benchtop.ROW_HSIZE]
					nextOffset := binary.LittleEndian.Uint32(header[:benchtop.ROW_OFFSET_HSIZE])
					bSize := binary.LittleEndian.Uint32(header[benchtop.ROW_OFFSET_HSIZE:benchtop.ROW_HSIZE])
					if bSize == 0 {
						if nextOffset == 0 || nextOffset <= offset {
							break
						}
						offset = nextOffset
						continue
					}
					jsonStart := offset + benchtop.ROW_HSIZE
					jsonEnd := jsonStart + bSize
					if jsonEnd > uint32(len(m)) {
						break
					}
					rowData := m[jsonStart:jsonEnd]
					if err := b.processJSONRowDataId(rowData, filter, outChan); err != nil {
						log.Debugf("skip row in section %d: %v", sec.ID, err)
					}
					if nextOffset == 0 || nextOffset <= offset {
						break
					}
					offset = nextOffset
				}
			}(sec)
		}
	}
	go func() { wg.Wait(); close(outChan) }()
	return outChan
}

// processJSONRowDataId handles parsing of row bytes for ScanId, applying filters, and sending IDs to the output channel.
func (b *JSONTable) processJSONRowDataId(rowData []byte, filter benchtop.RowFilter, outChan chan string) error {
	newData, err := zstd.Decompress(nil, rowData)
	if err != nil {
		return err
	}

	if filter != nil && !filter.IsNoOp() {
		if !filter.Matches(newData, b.Name) {
			return nil
		}
	}

	node, err := sonic.Get(newData, "1")
	if err != nil {
		log.Errorf("Error accessing JSON path for row data %s: %v\n", string(newData), err)
		return err
	}

	ID, err := node.String()
	if err != nil {
		log.Errorf("Error unmarshaling node: %v\n", err)
		return err
	}

	outChan <- ID
	return nil
}

/*
func (b *JSONTable) CompactSection(secId uint16) error {
	sec, exists := b.Sections[secId]
	if !exists {
		return fmt.Errorf("section %d not found", secId)
	}
	sec.Lock.Lock()
	defer sec.Lock.Unlock()

	flushCounter := 0
	tempFileName := sec.Path + ".compact"
	tempHandle, err := os.Create(tempFileName)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempHandle.Close()

	m, err := mmap.Map(sec.File, mmap.RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to map file: %w", err)
	}
	defer m.Unmap()

	writer := bufio.NewWriterSize(tempHandle, 16*1024*1024)
	var newOffset uint32 = 0
	inputChan := make(chan benchtop.Index, 100)

	// todo: figure out how to set indices from the driver instead of the table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.setDataIndices(inputChan)
	}()

	var offset uint32 = 0
	for offset+benchtop.ROW_HSIZE <= uint32(len(m)) {
		header := m[offset : offset+benchtop.ROW_HSIZE]
		nextOffset := binary.LittleEndian.Uint32(header[:benchtop.ROW_OFFSET_HSIZE])
		bSize := binary.LittleEndian.Uint32(header[benchtop.ROW_OFFSET_HSIZE:benchtop.ROW_HSIZE])

		if bSize == 0 || int64(nextOffset) == int64(benchtop.ROW_HSIZE) {
			if int64(nextOffset) > int64(offset) {
				offset = nextOffset
			}
			continue
		}

		jsonStart := offset + benchtop.ROW_HSIZE
		jsonEnd := jsonStart + bSize
		if jsonEnd > uint32(len(m)) {
			return fmt.Errorf("incomplete JSON data at section %d, offset %d, size %d", sec.ID, offset, bSize)
		}

		rowData := m[jsonStart:jsonEnd]

		rowData, err := zstd.Decompress(nil, rowData)
		if err != nil {
			log.Debugf("Failed to decompress row at section %d, offset %d: %v", sec.ID, offset, err)
			if nextOffset == 0 || nextOffset <= offset {
				break
			}
			offset = nextOffset
			continue
		}

		var mRow RowData
		err = sonic.ConfigFastest.Unmarshal(rowData, &mRow)
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("JSON data for row at section %d, offset %d, size %d was incomplete: %w", sec.ID, offset, bSize, err)
			}
			return fmt.Errorf("failed to decode JSON row at section %d, offset %d, size %d: %w", sec.ID, offset, bSize, err)
		}

		node, err := sonic.Get(rowData, "1")
		if err != nil {
			return fmt.Errorf("failed to access ID field at section %d, offset %d: %w", sec.ID, offset, err)
		}
		key, err := node.String()
		if err != nil {
			return fmt.Errorf("failed to unmarshal ID field at section %d, offset %d: %w", sec.ID, offset, err)
		}
		inputChan <- benchtop.Index{Key: []byte(key), Loc: benchtop.RowLoc{Offset: newOffset, Size: bSize}}

		newOffsetBytes := make([]byte, benchtop.ROW_OFFSET_HSIZE)
		binary.LittleEndian.PutUint32(newOffsetBytes, newOffset+bSize+benchtop.ROW_HSIZE)
		_, err = writer.Write(newOffsetBytes)
		if err != nil {
			return fmt.Errorf("failed writing new offset at %d: %w", newOffset, err)
		}
		_, err = writer.Write(rowData)
		if err != nil {
			return fmt.Errorf("failed writing JSON row at offset %d: %w", newOffset, err)
		}

		flushCounter++
		if flushCounter%FLUSH_THRESHOLD == 0 {
			if err := writer.Flush(); err != nil {
				return fmt.Errorf("failed flushing writer: %w", err)
			}
		}
		newOffset += bSize + benchtop.ROW_HSIZE
	}
	close(inputChan)
	//wg.Wait()

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed final flush: %w", err)
	}
	if err := tempHandle.Sync(); err != nil {
		return fmt.Errorf("failed syncing temp file: %w", err)
	}
	if err := tempHandle.Close(); err != nil {
		return fmt.Errorf("failed closing temp file: %w", err)
	}
	if err := sec.File.Close(); err != nil {
		return fmt.Errorf("failed closing old handle: %w", err)
	}

	if err := os.Rename(tempFileName, sec.Path); err != nil {
		return fmt.Errorf("failed renaming compacted file: %w", err)
	}

	newHandle, err := os.OpenFile(sec.Path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed reopening compacted file: %w", err)
	}
	sec.File = newHandle

	oldPool := sec.FilePool
	sec.FilePool = make(chan *os.File, cap(oldPool))
	for range cap(sec.FilePool) {
		file, err := os.OpenFile(sec.Path, os.O_RDWR, 0666)
		if err != nil {
			return fmt.Errorf("failed to refresh file pool: %w", err)
		}
		sec.FilePool <- file
	}
	close(oldPool)
	for file := range oldPool {
		file.Close()
	}

	// Reset stats
	stat, _ := os.Stat(sec.Path)
	sec.LiveBytes = uint32(stat.Size())
	sec.DeletedRows = 0
	// Note: Could set sec.Active = false and create new section, updating RowLocs in DB,
	// but current design reuses same section ID and path
	return nil
}

func (b *JSONTable) Compact() error {
	var errs *multierror.Error
	for secId, sec := range b.Sections {
		if float64(sec.DeletedRows)/float64(sec.TotalRows) > MAX_COMPACT_RATIO {
			if err := b.CompactSection(secId); err != nil {
				errs = multierror.Append(errs, err)
			}
		}
	}
	return errs.ErrorOrNil()
}
*/

func ConvertJSONPathToArray(path string) ([]any, error) {
	path = strings.TrimLeft(path, "./")
	if path == "" {
		return []any{"0"}, nil // Handle empty path after trimming
	}

	result := make([]any, 1, len(path)/2+1)
	result[0] = "0"
	var start int = 0
	var length int = len(path)

	for i := 0; i < length; i++ {
		char := path[i]

		switch char {
		case '.':
			// Found a dot separator. The preceding characters (if any) are a key.
			if i > start {
				token := path[start:i]
				if token != "" {
					result = append(result, token)
				}
			}
			start = i + 1 // Start the next token after the dot

		case '[':
			// Found the start of an array index. The preceding characters (if any) are a key.
			if i > start {
				token := path[start:i]
				if token != "" {
					result = append(result, token)
				}
			}

			// Look for the closing bracket
			j := i + 1
			for j < length && path[j] != ']' {
				j++
			}

			if j == length || j == i+1 {
				// Error: missing closing bracket or empty brackets '[]'
				return nil, fmt.Errorf("invalid path format: missing array closing bracket or empty index at position %d", i)
			}

			// Extract and convert the index string
			numStr := path[i+1 : j]
			index, err := strconv.Atoi(numStr)
			if err != nil {
				return nil, fmt.Errorf("invalid array index: %s", numStr)
			}
			result = append(result, index)

			// Skip past the index token, including the ']'
			i = j // Loop's i++ will make it j+1
			start = i + 1
		}
	}

	// Handle the final token if the path didn't end with a separator
	if start < length {
		token := path[start:length]
		if token != "" {
			result = append(result, token)
		}
	}

	return result, nil
}

func (b *JSONTable) GetRows(locs []*benchtop.RowLoc, sectionId uint16) ([]map[string]any, []error) {
	results := make([]map[string]any, len(locs))
	errors := make([]error, len(locs))
	sec, exists := b.Sections[sectionId]
	if !exists || len(sec.MMap) == 0 {
		return nil, []error{fmt.Errorf("sectionId not found in sections: %d", sectionId)}
	}

	sec.Lock.RLock()
	defer sec.Lock.RUnlock()
	var wg sync.WaitGroup
	sem := make(chan struct{}, runtime.NumCPU()) // Per-section concurrency
	chunkSize := 100                             // Adjust based on profiling
	for i := 0; i < len(locs); i += chunkSize {
		end := i + chunkSize
		if end > len(locs) {
			end = len(locs)
		}
		chunk := locs[i:end]
		wg.Add(1)
		go func(start int, chunk []*benchtop.RowLoc) {
			sem <- struct{}{}
			defer func() { <-sem; wg.Done() }()
			for j, loc := range chunk {
				idx := start + j
				if loc.Section != sectionId {
					errors[idx] = fmt.Errorf("Expected sectionId %d but got %d instead", sectionId, loc.Section)
					continue
				}
				startOffset := loc.Offset + benchtop.ROW_HSIZE
				endOffset := startOffset + loc.Size
				if endOffset > uint32(len(sec.MMap)) {
					errors[idx] = fmt.Errorf("row out of bounds: %d > %d", endOffset, len(sec.MMap))
					continue
				}
				compressed := sec.MMap[startOffset:endOffset]
				decompressed, err := zstd.Decompress(nil, compressed)
				if err != nil {
					errors[idx] = fmt.Errorf("decompress failed: %w", err)
					continue
				}
				var m RowData
				if err := sonic.ConfigFastest.Unmarshal(decompressed, &m); err != nil {
					errors[idx] = fmt.Errorf("unmarshal failed: %w", err)
					continue
				}
				results[idx] = m.Data
			}
		}(i, chunk)
	}
	wg.Wait()
	return results, errors
}

/*func (b *JSONTable) GetRows(locs []*benchtop.RowLoc, sectionId uint16) ([]map[string]any, []error) {
	results := make([]map[string]any, len(locs))
	errors := make([]error, len(locs))
	sec, exists := b.Sections[sectionId]
	if !exists || len(sec.MMap) == 0 {
		return nil, []error{fmt.Errorf("sectionId not found in sections: %d", sectionId)}
	}

	sec.Lock.RLock()
	defer sec.Lock.RUnlock()
	var m RowData
	var start, end uint32 = 0, 0
	for i, loc := range locs {
		if loc.Section != sectionId {
			errors[i] = fmt.Errorf("Expected sectionId %d but got %d instead", sectionId, loc.Section)
			continue
		}
		start = loc.Offset + benchtop.ROW_HSIZE
		end = start + loc.Size
		if end > uint32(len(sec.MMap)) {
			errors[i] = fmt.Errorf("row out of bounds: %d > %d", end, len(sec.MMap))
			continue
		}
		decompressed, err := zstd.Decompress(nil, sec.MMap[start:end])
		if err != nil {
			errors[i] = fmt.Errorf("decompress failed: %w", err)
			continue
		}
		if err := sonic.ConfigFastest.Unmarshal(decompressed, &m); err != nil {
			errors[i] = fmt.Errorf("unmarshal failed: %w", err)
			continue
		}
		results[i] = m.Data
	}
	return results, errors
}*/

func (b *JSONTable) GetColumnDefs() []benchtop.ColumnDef {
	return b.Columns
}
