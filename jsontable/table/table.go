package table

import (
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
func DefaultPartitionFunc(numPartitions uint32) func(id []byte) uint8 {
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
	//fmt.Println("SECTION: %v\n", sec)

	bData, err := sonic.ConfigFastest.Marshal(b.PackData(elem.Data, string(elem.Id)))
	if err != nil {
		return nil, err
	}
	totalSize := uint32(len(bData)) + benchtop.ROW_HSIZE

	sec.Lock.Lock()

	// Check size and rotate if needed
	if sec.LiveBytes+totalSize > section.MAX_SECTION_SIZE {
		// Close current
		sec.CloseSection()
		//sec.RemapReadOnly() // now RDONLY

		// Create new active
		newSec, err := b.CreateNewSection(partitionId)
		if err != nil {
			sec.Lock.Unlock()
			return nil, err
		}
		sec = newSec
		b.ActiveSections[partitionId] = sec
	}

	loc, err := sec.WriteJsonEntryToSection(bData)
	if err != nil {
		sec.Lock.Unlock()
		return nil, err
	}

	sec.TotalRows++
	sec.Lock.Unlock()

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
	for i := 0; i < cap(filePool); i++ {
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

	// --- DIRECT MMAP ACCESS ---
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

	out, err := b.unpackData(true, false, &m)
	if err != nil {
		return nil, err
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
	_, err = sec.File.Write(bytes.Repeat([]byte{0x00}, 4))
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
					if err := b.processJSONRowData(rowData, loadData, filter, outChan); err != nil {
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
