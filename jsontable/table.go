package jsontable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
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

const (
	MaxSectionSize             = 1 << 29 // 512MB
	CompactionThreshold        = 0.2     // 20% deleted rows triggers compaction
	SectionFileSuffix          = ".partition"
	ROW_HSIZE           uint32 = 8 // Header size: 8-byte next offset + 4-byte size
	ROW_OFFSET_HSIZE    uint32 = 4 // Offset part of header
)

type JSONTable struct {
	Pb        *pebblebulk.PebbleKV
	db        *pebble.DB
	columns   []benchtop.ColumnDef
	columnMap map[string]int
	TableId   uint16
	Path      string // Base path (for legacy single file)
	Name      string
	FileName  string // Base name for section files

	Sections              map[uint16]*Section   // sectionId -> Section
	PartitionMap          map[uint8][]uint16    // partitionId -> []sectionId
	SectionLock           sync.Mutex            // For creating new sections
	NumPartitions         uint32                // Number of partitions
	PartitionFunc         func(id []byte) uint8 // Assigns row to partition
	MaxSectionSize        uint32                // Max size per section
	MaxConcurrentSections uint8                 // Limit for parallel operations
}

// Section represents a physical file within a partition
type Section struct {
	ID          uint16        // Global unique section ID (for RowLoc)
	PartitionID uint8         // Partition this section belongs to
	Path        string        // File path (e.g., table.data.partition0.section1)
	Handle      *os.File      // Main file handle for writes
	FilePool    chan *os.File // Pool for read/write access
	Lock        sync.RWMutex  // Per-section lock
	TotalRows   uint32        // Total rows (live + deleted)
	DeletedRows uint32        // Deleted rows (for compaction trigger)
	LiveBytes   uint32        // Live data size (bytes)
	Active      bool          // True unless compacted/merged
}

func (b *JSONTable) Init(poolSize int) error {
	if b.NumPartitions == 0 {
		b.NumPartitions = 4
	}
	if b.PartitionFunc == nil {
		b.PartitionFunc = DefaultPartitionFunc(b.NumPartitions)
	}
	b.Sections = make(map[uint16]*Section)
	b.PartitionMap = make(map[uint8][]uint16)
	b.MaxSectionSize = MaxSectionSize
	b.MaxConcurrentSections = uint8(runtime.NumCPU())

	dir := filepath.Dir(b.FileName)
	base := filepath.Base(b.FileName)
	files, err := os.ReadDir(dir)
	//log.Debugln("Files in Dir: ", files)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}
	var secIdCounter uint16 = 0
	for _, f := range files {
		if strings.HasPrefix(f.Name(), base+SectionFileSuffix) {
			parts := strings.Split(strings.TrimPrefix(f.Name(), base+SectionFileSuffix), ".section")
			if len(parts) != 2 {
				continue
			}
			pId, err := strconv.Atoi(parts[0])
			if err != nil {
				continue
			}
			p8Id := uint8(pId)
			secPath := filepath.Join(dir, f.Name())
			sec := &Section{
				ID:          secIdCounter,
				PartitionID: p8Id,
				Path:        secPath,
				Active:      true,
			}
			sec.FilePool = make(chan *os.File, poolSize)
			for i := range poolSize {
				file, err := os.OpenFile(secPath, os.O_RDWR, 0666)
				if err != nil {
					for range i {
						if file, ok := <-sec.FilePool; ok {
							file.Close()
						}
					}
					return fmt.Errorf("failed to init file pool for %s: %w", secPath, err)
				}
				sec.FilePool <- file
			}
			if sec.Handle, err = os.OpenFile(secPath, os.O_RDWR, 0666); err != nil {
				return fmt.Errorf("failed to open section handle: %w", err)
			}
			if stat, err := os.Stat(secPath); err == nil {
				sec.LiveBytes = uint32(stat.Size())
			}
			b.Sections[secIdCounter] = sec
			b.PartitionMap[p8Id] = append(b.PartitionMap[p8Id], secIdCounter)
			secIdCounter++
		}
	}

	// Ensure at least one section per partition
	for pId := uint8(0); pId < uint8(b.NumPartitions); pId++ {
		if len(b.PartitionMap[pId]) == 0 {
			b.createNewSection(pId)
		}
	}

	return nil
}

func (b *JSONTable) createNewSection(partitionId uint8) *Section {
	b.SectionLock.Lock()
	defer b.SectionLock.Unlock()

	secId := uint16(len(b.Sections))               // Global unique ID
	localSecId := len(b.PartitionMap[partitionId]) // Local to partition
	path := fmt.Sprintf("%s%s%d.section%d", b.FileName, SectionFileSuffix, partitionId, localSecId)
	handle, err := os.Create(path)
	if err != nil {
		log.Errorf("Failed to create section file %s: %v", path, err)
		return nil
	}

	sec := &Section{
		ID:          secId,
		PartitionID: partitionId,
		Path:        path,
		Handle:      handle,
		Active:      true,
	}
	sec.FilePool = make(chan *os.File, 10)
	for range cap(sec.FilePool) {
		file, err := os.OpenFile(path, os.O_RDWR, 0666)
		if err != nil {
			log.Errorf("Failed to init file pool for %s: %v", path, err)
			return nil
		}
		sec.FilePool <- file
	}

	b.Sections[secId] = sec
	b.PartitionMap[partitionId] = append(b.PartitionMap[partitionId], secId)
	return sec
}

// DefaultPartitionFunc assigns rows to partitions using FNV hash
func DefaultPartitionFunc(numPartitions uint32) func(id []byte) uint8 {
	return func(id []byte) uint8 {
		h := fnv.New32a()
		h.Write(id)
		return uint8(h.Sum32() % numPartitions)
	}
}

func (b *JSONTable) GetColumnDefs() []benchtop.ColumnDef {
	return b.columns
}

func (b *JSONTable) Close() {
	for _, sec := range b.Sections {
		if sec.Handle != nil {
			sec.Handle.Sync()
			sec.Handle.Close()
		}
		if sec.FilePool != nil {
			for len(sec.FilePool) > 0 {
				if file, ok := <-sec.FilePool; ok {
					file.Sync()
					file.Close()
				}
			}
			close(sec.FilePool)
		}
	}
}

/*
////////////////////////////////////////////////////////////////
Unary single effect operations
*/

func (b *JSONTable) AddRow(elem benchtop.Row) (*benchtop.RowLoc, error) {
	partitionId := b.PartitionFunc(elem.Id)
	if partitionId < 0 || partitionId >= uint8(b.NumPartitions) {
		return nil, fmt.Errorf("invalid partition ID: %d", partitionId)
	}

	if len(b.PartitionMap[uint8(partitionId)]) == 0 {
		b.createNewSection(partitionId)
	}
	secId := b.PartitionMap[partitionId][len(b.PartitionMap[partitionId])-1]
	sec, exists := b.Sections[secId]
	if !exists {
		return nil, fmt.Errorf("section %d not found", secId)
	}

	// Step 3: Check size, create new section if needed
	sec.Lock.Lock()
	if sec.LiveBytes > b.MaxSectionSize {
		sec.Lock.Unlock()
		sec = b.createNewSection(partitionId)
		if sec == nil {
			return nil, fmt.Errorf("failed to create new section for partition %d", partitionId)
		}
		sec.Lock.Lock()
	}
	defer sec.Lock.Unlock()

	// Step 4: Marshal and write data to section file
	bData, err := sonic.ConfigFastest.Marshal(b.packData(elem.Data, string(elem.Id)))
	if err != nil {
		return nil, err
	}
	offset, err := sec.Handle.Seek(0, io.SeekEnd) // Append to end
	if err != nil {
		return nil, err
	}
	writesize, err := b.writeJsonEntryToSection(sec, uint32(offset), bData)
	if err != nil {
		log.Errorf("write handler err in AddRow: %v", err)
		return nil, err
	}

	// Step 5: Update stats and RowLoc
	sec.TotalRows++
	sec.LiveBytes += uint32(writesize + ROW_HSIZE)
	loc := &benchtop.RowLoc{
		Section: uint16(sec.ID),
		Offset:  uint32(offset),
		Size:    uint32(writesize),
	}
	err = b.AddTableEntryInfo(nil, elem.Id, loc)
	if err != nil {
		return nil, err
	}

	return loc, nil
}

func (b *JSONTable) writeJsonEntryToSection(sec *Section, offset uint32, data []byte) (uint32, error) {
	nextOffset := offset + uint32(len(data)) + ROW_HSIZE
	payload := make([]byte, ROW_HSIZE+uint32(len(data)))
	binary.LittleEndian.PutUint32(payload[:ROW_OFFSET_HSIZE], uint32(nextOffset))
	binary.LittleEndian.PutUint32(payload[ROW_OFFSET_HSIZE:], uint32(len(data)))
	copy(payload[ROW_HSIZE:], data)

	_, err := sec.Handle.WriteAt(payload, int64(offset))
	if err != nil {
		return 0, fmt.Errorf("failed to write payload: %w", err)
	}
	return uint32(len(data)), nil
}

func (b *JSONTable) GetRow(loc *benchtop.RowLoc) (map[string]any, error) {
	sec, exists := b.Sections[loc.Section]
	if !exists {
		return nil, fmt.Errorf("section %d not found", loc.Section)
	}

	file := <-sec.FilePool
	defer func() { sec.FilePool <- file }()

	_, err := file.Seek(int64(loc.Offset+ROW_HSIZE), io.SeekStart)
	if err != nil {
		return nil, err
	}

	decoder := sonic.ConfigFastest.NewDecoder(io.LimitReader(file, int64(loc.Size)))
	var m RowData
	err = decoder.Decode(&m)
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("JSON data for row at section %d, offset %d, size %d was incomplete: %w", loc.Section, loc.Offset, loc.Size, err)
		}
		return nil, fmt.Errorf("failed to decode JSON row at section %d, offset %d, size %d: %w", loc.Section, loc.Offset, loc.Size, err)
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

	_, err := file.WriteAt([]byte{0x00, 0x00, 0x00, 0x00}, int64(loc.Offset+ROW_OFFSET_HSIZE))
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

	_, err := sec.Handle.WriteAt([]byte{0x00, 0x00, 0x00, 0x00}, int64(loc.Offset+ROW_OFFSET_HSIZE))
	if err != nil {
		return fmt.Errorf("writeAt failed: %w", err)
	}
	err = b.db.Delete(benchtop.NewPosKey(b.TableId, id), nil)
	if err != nil {
		return err
	}
	sec.DeletedRows++
	sec.LiveBytes -= loc.Size
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
				continue // Skip missing sections
			}
			wg.Add(1)
			go func(sec *Section) {
				sem <- struct{}{}
				handle := <-sec.FilePool
				defer func() {
					<-sem
					wg.Done()
					sec.FilePool <- handle
				}()
				_, err := handle.Seek(0, io.SeekStart)
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
				for offset+ROW_HSIZE <= uint32(len(m)) {
					header := m[offset : offset+ROW_HSIZE]
					nextOffset := binary.LittleEndian.Uint32(header[:ROW_OFFSET_HSIZE])
					bSize := binary.LittleEndian.Uint32(header[ROW_OFFSET_HSIZE:ROW_HSIZE])

					// Skip deleted rows (bSize == 0)
					if bSize == 0 {
						if nextOffset == 0 || nextOffset <= offset {
							break // End of valid data or corrupted nextOffset
						}
						offset = nextOffset
						continue
					}

					jsonStart := offset + ROW_HSIZE
					jsonEnd := jsonStart + bSize
					// Ensure the row data fits within the file
					if jsonEnd > uint32(len(m)) {
						log.Debugf("Incomplete record at section %d, offset %d", sec.ID, offset)
						break
					}

					rowData := m[jsonStart:jsonEnd]
					err = b.processJSONRowData(rowData, loadData, filter, outChan)
					if err != nil {
						log.Debugf("Skipping malformed row at section %d, offset %d: %v", sec.ID, offset, err)
					}

					// For the last row, nextOffset may be 0
					if nextOffset == 0 || nextOffset <= offset {
						break // Reached the end of valid rows
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
func (b *JSONTable) CompactSection(secId uint16) error {
	sec, exists := b.Sections[secId]
	if !exists {
		return fmt.Errorf("section %d not found", secId)
	}
	sec.Lock.Lock()
	defer sec.Lock.Unlock()

	const flushThreshold = 1000
	flushCounter := 0
	tempFileName := sec.Path + ".compact"
	tempHandle, err := os.Create(tempFileName)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempHandle.Close()

	m, err := mmap.Map(sec.Handle, mmap.RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to map file: %w", err)
	}
	defer m.Unmap()

	writer := bufio.NewWriterSize(tempHandle, 16*1024*1024)
	var newOffset uint32 = 0
	inputChan := make(chan benchtop.Index, 100)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.setDataIndices(inputChan)
	}()

	var offset uint32 = 0
	for offset+ROW_HSIZE <= uint32(len(m)) {
		header := m[offset : offset+ROW_HSIZE]
		nextOffset := binary.LittleEndian.Uint32(header[:ROW_OFFSET_HSIZE])
		bSize := binary.LittleEndian.Uint32(header[ROW_OFFSET_HSIZE:ROW_HSIZE])

		if bSize == 0 || int64(nextOffset) == int64(ROW_HSIZE) {
			if int64(nextOffset) > int64(offset) {
				offset = nextOffset
			}
			continue
		}

		jsonStart := offset + ROW_HSIZE
		jsonEnd := jsonStart + bSize
		if jsonEnd > uint32(len(m)) {
			return fmt.Errorf("incomplete JSON data at section %d, offset %d, size %d", sec.ID, offset, bSize)
		}

		rowData := m[jsonStart:jsonEnd]
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

		newOffsetBytes := make([]byte, ROW_OFFSET_HSIZE)
		binary.LittleEndian.PutUint32(newOffsetBytes, newOffset+bSize+ROW_HSIZE)
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
		newOffset += bSize + ROW_HSIZE
	}
	close(inputChan)
	wg.Wait()

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed final flush: %w", err)
	}
	if err := tempHandle.Sync(); err != nil {
		return fmt.Errorf("failed syncing temp file: %w", err)
	}
	if err := tempHandle.Close(); err != nil {
		return fmt.Errorf("failed closing temp file: %w", err)
	}
	if err := sec.Handle.Close(); err != nil {
		return fmt.Errorf("failed closing old handle: %w", err)
	}

	if err := os.Rename(tempFileName, sec.Path); err != nil {
		return fmt.Errorf("failed renaming compacted file: %w", err)
	}

	newHandle, err := os.OpenFile(sec.Path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed reopening compacted file: %w", err)
	}
	sec.Handle = newHandle

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
		if float64(sec.DeletedRows)/float64(sec.TotalRows) > CompactionThreshold {
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
