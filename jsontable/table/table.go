package table

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/DataDog/zstd"
	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/section"
	"github.com/bmeg/grip/log"
	"github.com/edsrzf/mmap-go"
	multierror "github.com/hashicorp/go-multierror"

	"github.com/bytedance/sonic"
)

const (
	PART_FILE_SUFFIX    string = ".partition"
	SECTION_FILE_SUFFIX string = ".section"
	SECTION_ID_MULT     uint16 = 256
	MAX_SECTION_SIZE           = 1 << 26 // 64MB
	MAX_COMPACT_RATIO          = 0.2     // 20% deleted rows triggers compaction
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
}

// DefaultPartitionFunc assigns rows to partitions using FNV hash
func DefaultPartitionFunc(numPartitions uint32) func(id []byte) uint8 {
	return func(id []byte) uint8 {
		h := fnv.New32a()
		h.Write(id)
		return uint8(h.Sum32() % numPartitions)
	}
}

func (b *JSONTable) Close() {
	for _, sec := range b.Sections {
		if sec.Writer != nil {
			sec.Writer.Close()
		}
		if sec.File != nil {
			sec.File.Close()
		}
		if sec.FilePool != nil {
			for len(sec.FilePool) > 0 {
				if file, ok := <-sec.FilePool; ok {
					file.Close()
				}
			}
			close(sec.FilePool)
		}
	}
	b.Fields = map[string]struct{}{}
}

// AddRow adds a single row to the JSONTable, writing it as zstd-compressed data.
func (b *JSONTable) AddRow(elem benchtop.Row) (*benchtop.RowLoc, error) {
	// partitionId returned as uint8
	partitionId := b.PartitionFunc(elem.Id)
	if int(partitionId) < 0 || partitionId >= uint8(b.NumPartitions) {
		return nil, fmt.Errorf("invalid partition ID: %d", partitionId)
	}

	// Ensure partition has at least one section
	if len(b.PartitionMap[uint8(partitionId)]) == 0 {
		_, err := b.CreateNewSection(partitionId)
		if err != nil {
			return nil, fmt.Errorf("failed to create new section for partition %d: %w", partitionId, err)
		}
	}

	secId := b.PartitionMap[partitionId][len(b.PartitionMap[partitionId])-1]
	sec, exists := b.Sections[secId]
	if !exists {
		return nil, fmt.Errorf("section %d not found", secId)
	}

	// Marshal the row payload to compute size
	bData, err := sonic.ConfigFastest.Marshal(b.PackData(elem.Data, string(elem.Id)))
	if err != nil {
		return nil, fmt.Errorf("failed to marshal row data for ID %s: %w", elem.Id, err)
	}
	totalUncompressedSize := uint32(len(bData)) + benchtop.ROW_HSIZE

	sec.Lock.Lock()
	defer sec.Lock.Unlock()

	if sec.LiveBytes+totalUncompressedSize > MAX_SECTION_SIZE {
		newSec, err := b.CreateNewSection(partitionId)
		if err != nil {
			return nil, fmt.Errorf("failed to create new section for partition %d: %w", partitionId, err)
		}
		sec = newSec

	}

	loc, err := sec.WriteJsonEntryToSection(bData)
	if err != nil {
		return nil, fmt.Errorf("failed to write payload for row %s in section %d: %w", elem.Id, sec.ID, err)
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

	// Ensure the generated ID is not already in use (edge case sanity check)
	if _, exists := b.Sections[secId]; exists {
		return nil, fmt.Errorf("section ID conflict: ID %d already exists", secId)
	}

	path := fmt.Sprintf("%s%s%d.section%d", b.FileName, PART_FILE_SUFFIX, partitionId, localSecId)
	handle, err := os.Create(path)
	if err != nil {
		log.Errorf("Failed to create section file %s: %v", path, err)
		return nil, err
	}

	sec := &section.Section{
		ID:          secId,
		PartitionID: partitionId,
		Path:        path,
		Writer:      zstd.NewWriter(handle),
		Active:      true,
		File:        handle,
	}
	sec.FilePool = make(chan *os.File, 10)
	for range cap(sec.FilePool) {
		file, err := os.OpenFile(path, os.O_RDWR, 0666)
		if err != nil {
			log.Errorf("Failed to init file pool for %s: %v", path, err)
			handle.Close() // Clean up the create handle
			return nil, err
		}
		sec.FilePool <- file
	}

	b.Sections[secId] = sec
	b.PartitionMap[partitionId] = append(b.PartitionMap[partitionId], secId)
	return sec, nil
}

func NewSecPayload(offset uint32, data []byte) []byte {
	dataLen := uint32(len(data))
	payload := make([]byte, benchtop.ROW_HSIZE+dataLen)
	nextOffset := offset + benchtop.ROW_HSIZE + dataLen
	binary.LittleEndian.PutUint32(payload[:benchtop.ROW_OFFSET_HSIZE], nextOffset)
	binary.LittleEndian.PutUint32(payload[benchtop.ROW_OFFSET_HSIZE:benchtop.ROW_HSIZE], dataLen)
	copy(payload[benchtop.ROW_HSIZE:], data)
	return payload
}

func (b *JSONTable) GetRow(loc *benchtop.RowLoc) (map[string]any, error) {
	sec, exists := b.Sections[loc.Section]
	if !exists {
		return nil, fmt.Errorf("section %d not found", loc.Section)
	}

	file := <-sec.FilePool
	defer func() { sec.FilePool <- file }()

	// Seek to the start of the row (uncompressed header + compressed data)
	_, err := file.Seek(int64(loc.Offset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to offset %d in section %d: %w", loc.Offset, loc.Section, err)
	}

	// Read the entire row in one go: header + compressed data
	rowData := make([]byte, benchtop.ROW_HSIZE+loc.Size)
	_, err = io.ReadFull(file, rowData)
	if err != nil {
		return nil, fmt.Errorf("failed to read row data at offset %d in section %d: %w", loc.Offset, loc.Section, err)
	}

	// Parse the 8-byte header
	header := rowData[:benchtop.ROW_HSIZE]
	// nextOffset := binary.LittleEndian.Uint32(header[:4]) // Ignored for now
	compressedSize := binary.LittleEndian.Uint32(header[4:8])

	// Verify compressed size matches loc.Size
	if compressedSize != loc.Size {
		return nil, fmt.Errorf("compressed size mismatch at offset %d in section %d: header says %d, RowLoc says %d", loc.Offset, loc.Section, compressedSize, loc.Size)
	}

	// Decompress the compressed section
	compressed := rowData[benchtop.ROW_HSIZE:]
	decompressed, err := zstd.Decompress(nil, compressed)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress data at offset %d in section %d: %w", loc.Offset, loc.Section, err)
	}

	// Decode the JSON
	var m RowData
	err = sonic.ConfigFastest.Unmarshal(decompressed, &m)
	if err != nil {
		return nil, fmt.Errorf("failed to decode JSON row at section %d, offset %d, size %d: %w", loc.Section, loc.Offset, loc.Size, err)
	}

	out, err := b.unpackData(true, false, &m)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack data for row at section %d, offset %d: %w", loc.Section, loc.Offset, err)
	}
	return out.(map[string]any), nil
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
	_, err = sec.Writer.Write(bytes.Repeat([]byte{0x00}, 4))
	if err != nil {
		return fmt.Errorf("writeAt failed: %w", err)
	}
	sec.DeletedRows++
	sec.LiveBytes -= loc.Size
	return nil
}

func (b *JSONTable) Scan(loadData bool, filter benchtop.RowFilter) chan any {
	outChan := make(chan any, 100*len(b.Sections))
	var wg sync.WaitGroup
	sem := make(chan struct{}, b.MaxConcurrentSections)

	partitions := make(map[uint8]bool)
	for i := uint8(0); i < uint8(b.NumPartitions); i++ {
		partitions[i] = true
	}

	for pId := range partitions {
		for _, secId := range b.PartitionMap[pId] {
			sec, exists := b.Sections[secId]
			if !exists {
				log.Debugf("SECTION: %s does not exist in %s", secId, b.Sections)
				continue
			}
			wg.Add(1)
			go func(sec *section.Section) {
				sem <- struct{}{}
				handle := <-sec.FilePool
				defer func() {
					<-sem
					sec.FilePool <- handle
					wg.Done()
				}()

				fileInfo, err := handle.Stat()
				if err != nil {
					log.Errorf("Error getting file info for section %d: %v", sec.ID, err)
					return
				}
				// Check for an empty file before attempting the mmap.
				if fileInfo.Size() == 0 {
					log.Debugf("Skipping empty file for section %d (%s).", sec.ID, handle.Name())
					return
				}
				_, err = handle.Seek(0, io.SeekStart)
				if err != nil {
					log.Errorln("Error in scan seek:", err)
					return
				}
				m, err := mmap.Map(handle, mmap.RDONLY, 0)
				if err != nil {
					log.Errorln("Error mapping file:", err)
					return
				}
				defer m.Unmap()

				var offset uint32 = 0
				for offset+benchtop.ROW_HSIZE <= uint32(len(m)) {
					header := m[offset : offset+benchtop.ROW_HSIZE]
					nextOffset := binary.LittleEndian.Uint32(header[:benchtop.ROW_OFFSET_HSIZE])
					bSize := binary.LittleEndian.Uint32(header[benchtop.ROW_OFFSET_HSIZE:benchtop.ROW_HSIZE])

					// Skip deleted rows (bSize == 0)
					if bSize == 0 {
						if nextOffset == 0 || nextOffset <= offset {
							break // End of valid data or corrupted nextOffset
						}
						offset = nextOffset
						continue
					}

					/*if bSize == 0 || bSize == nextOffset-benchtop.ROW_HSIZE {
						offset = nextOffset
						continue
					}*/

					jsonStart := offset + benchtop.ROW_HSIZE
					jsonEnd := jsonStart + bSize
					// Ensure the row data fits within the file
					//fmt.Println("START: ", jsonStart, "END: ", jsonEnd, "SEC: ", sec.ID)
					if jsonEnd > uint32(len(m)) {
						log.Debugf("Incomplete record at section %d, offset %d", sec.ID, offset)
						break
					}

					rowData := m[jsonStart:jsonEnd]
					err = b.processJSONRowData(rowData, loadData, filter, outChan)
					if err != nil {
						log.Debugf("Skipping malformed row at section %d, offset %d: %v", sec.ID, offset, err)
					}

					if nextOffset == 0 || nextOffset <= offset {
						break
					}
					offset = nextOffset
				}
			}(sec)
		}
	}
	go func() {
		wg.Wait()
		close(outChan)
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

	newData, err := zstd.Decompress(nil, rowData)
	if err != nil {
		return err
	}

	if loadData || filter != nil && !filter.IsNoOp() {
		var m RowData

		sonic.ConfigFastest.Unmarshal(newData, &m)
		val, err = b.unpackData(true, true, &m)
		if err != nil {
			return err
		}
	} else {
		val = newData
	}

	if filter == nil || filter.IsNoOp() || (!filter.IsNoOp() && filter.Matches(val)) {
		if loadData {
			outChan <- val
			return nil
		}

		node, err := sonic.Get(newData, "1")
		if err != nil {
			log.Errorf("Error accessing JSON path for row data %s: %v\n", string(newData), err)
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

	/* todo: figure out how to set indices from the driver instead of the table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.setDataIndices(inputChan)
	}()
	*/

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

func (b *JSONTable) GetColumnDefs() []benchtop.ColumnDef {
	return b.Columns
}
