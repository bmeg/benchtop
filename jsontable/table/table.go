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
	"github.com/bmeg/benchtop/jsontable/block"
	"github.com/bmeg/benchtop/jsontable/section"
	"github.com/bmeg/grip/log"
	"github.com/edsrzf/mmap-go"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/sirupsen/logrus"

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

func (b *JSONTable) AddRow(elem benchtop.Row) (*benchtop.RowLoc, error) {
	partitionId := b.PartitionFunc(elem.Id)
	if int(partitionId) < 0 || partitionId >= uint8(b.NumPartitions) {
		return nil, fmt.Errorf("invalid partition ID: %d", partitionId)
	}

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

	bData, err := sonic.ConfigFastest.Marshal(b.PackData(elem.Data, string(elem.Id)))
	if err != nil {
		return nil, fmt.Errorf("failed to marshal row data for ID %s: %w", elem.Id, err)
	}

	// Estimated add size (uncompressed, conservative)
	estimatedAddSize := uint32(len(bData)) + 4

	sec.Lock.Lock()
	if sec.LiveBytes+estimatedAddSize > MAX_SECTION_SIZE {
		newSec, err := b.CreateNewSection(partitionId)
		if err != nil {
			sec.Lock.Unlock()
			return nil, fmt.Errorf("failed to create new section for partition %d: %w", partitionId, err)
		}
		sec = newSec
	}
	sec.Lock.Unlock()

	loc, err := sec.AppendRow(bData)
	if err != nil {
		return nil, fmt.Errorf("failed to append row %s to block in section %d: %w", elem.Id, sec.ID, err)
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
		ID:           secId,
		PartitionID:  partitionId,
		Path:         path,
		Writer:       zstd.NewWriter(handle),
		Active:       true,
		File:         handle,
		CurrentBlock: block.NewMemBlock(),
		Blocks:       []section.BlockMeta{},
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

func (b *JSONTable) GetRow(loc *benchtop.RowLoc) (map[string]any, error) {
	sec, exists := b.Sections[loc.Section]
	if !exists {
		return nil, fmt.Errorf("section %d not found", loc.Section)
	}

	sec.Lock.RLock()
	defer sec.Lock.RUnlock()

	// Check if the row is in the in-memory CurrentBlock
	if int(loc.BlockIndex) == len(sec.Blocks) && sec.CurrentBlock != nil {
		if int(loc.RowIndex) >= len(sec.CurrentBlock.Rows) {
			return nil, fmt.Errorf("row index %d out of range in current block of section %d", loc.RowIndex, loc.Section)
		}
		rowData := sec.CurrentBlock.Rows[loc.RowIndex]
		if len(rowData) == 0 {
			return nil, fmt.Errorf("row %d in current block of section %d is deleted or invalid", loc.RowIndex, loc.Section)
		}
		// Verify row size matches loc.Size
		if uint32(len(rowData)) != loc.Size {
			return nil, fmt.Errorf("row size mismatch in current block of section %d: expected %d, got %d", loc.Section, loc.Size, len(rowData))
		}
		// Decode the JSON (no decompress needed)
		var m RowData
		err := sonic.ConfigFastest.Unmarshal(rowData, &m)
		if err != nil {
			return nil, fmt.Errorf("failed to decode JSON row %d in current block of section %d: %w", loc.RowIndex, loc.Section, err)
		}
		out, err := b.unpackData(true, false, &m)
		if err != nil {
			return nil, fmt.Errorf("failed to unpack data for row %d in current block of section %d: %w", loc.RowIndex, loc.Section, err)
		}
		return out.(map[string]any), nil
	}

	// Ensure BlockIndex is valid for flushed blocks
	if int(loc.BlockIndex) >= len(sec.Blocks) {
		return nil, fmt.Errorf("block index %d out of range in section %d", loc.BlockIndex, loc.Section)
	}

	// Handle flushed block on disk
	file := <-sec.FilePool
	defer func() { sec.FilePool <- file }()

	// Get block metadata
	blockMeta := sec.Blocks[loc.BlockIndex]

	// Seek to the start of the block
	_, err := file.Seek(int64(blockMeta.FileOffset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to block offset %d in section %d: %w", blockMeta.FileOffset, loc.Section, err)
	}

	// Read the compressed block
	cData := make([]byte, blockMeta.Size)
	_, err = io.ReadFull(file, cData)
	if err != nil {
		return nil, fmt.Errorf("failed to read compressed block data at offset %d in section %d: %w", blockMeta.FileOffset, loc.Section, err)
	}

	// Decompress the block
	decompressed, err := zstd.Decompress(nil, cData)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress block at offset %d in section %d: %w", blockMeta.FileOffset, loc.Section, err)
	}

	// Create iterator for the decompressed block
	it := block.NewMemBlock().Iterator(decompressed)

	// Iterate to the desired row
	var rowData []byte
	for i := uint16(0); i <= loc.RowIndex; i++ {
		data, err := it.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to read row %d in block %d of section %d: %w", loc.RowIndex, loc.BlockIndex, loc.Section, err)
		}
		if i == loc.RowIndex {
			rowData = data
		}
	}

	// Verify row size matches loc.Size
	if uint32(len(rowData)) != loc.Size {
		return nil, fmt.Errorf("row size mismatch in block %d of section %d: expected %d, got %d", loc.BlockIndex, loc.Section, loc.Size, len(rowData))
	}

	// Decode the JSON
	var m RowData
	err = sonic.ConfigFastest.Unmarshal(rowData, &m)
	if err != nil {
		return nil, fmt.Errorf("failed to decode JSON row %d in block %d of section %d: %w", loc.RowIndex, loc.BlockIndex, loc.Section, err)
	}

	out, err := b.unpackData(true, false, &m)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack data for row %d in block %d of section %d: %w", loc.RowIndex, loc.BlockIndex, loc.Section, err)
	}
	return out.(map[string]any), nil
}

func (b *JSONTable) MarkDeleteTable(loc *benchtop.RowLoc) error {
	sec, exists := b.Sections[loc.Section]
	if !exists {
		return fmt.Errorf("section %d not found", loc.Section)
	}

	// Handle in-memory block
	if int(loc.BlockIndex) == len(sec.Blocks) {
		sec.Lock.Lock()
		defer sec.Lock.Unlock()
		if sec.CurrentBlock == nil || int(loc.RowIndex) >= len(sec.CurrentBlock.Rows) {
			return fmt.Errorf("row index %d out of range in current block of section %d", loc.RowIndex, loc.Section)
		}
		sec.CurrentBlock.Size -= uint32(len(sec.CurrentBlock.Rows[loc.RowIndex]))
		sec.CurrentBlock.Rows[loc.RowIndex] = []byte{} // Mark as deleted with empty slice
		sec.DeletedRows++
		sec.LiveBytes -= loc.Size
		return nil
	}

	// Handle flushed compressed block
	if int(loc.BlockIndex) >= len(sec.Blocks) {
		return fmt.Errorf("block index %d out of range in section %d", loc.BlockIndex, loc.Section)
	}

	file := <-sec.FilePool
	defer func() { sec.FilePool <- file }()

	sec.Lock.Lock()
	defer sec.Lock.Unlock()

	blockMeta := sec.Blocks[loc.BlockIndex]

	// Read compressed block
	_, err := file.Seek(int64(blockMeta.FileOffset), io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to block offset %d in section %d: %w", blockMeta.FileOffset, loc.Section, err)
	}
	cData := make([]byte, blockMeta.Size)
	_, err = io.ReadFull(file, cData)
	if err != nil {
		return fmt.Errorf("failed to read compressed block data at offset %d in section %d: %w", blockMeta.FileOffset, loc.Section, err)
	}

	// Decompress block
	decompressed, err := zstd.Decompress(nil, cData)
	if err != nil {
		return fmt.Errorf("failed to decompress block at offset %d in section %d: %w", blockMeta.FileOffset, loc.Section, err)
	}

	// Rebuild modified block with deleted row
	newBuf := new(bytes.Buffer)
	it := block.NewMemBlock().Iterator(decompressed)
	currentRow := uint16(0)
	for {
		rowData, err := it.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read row %d in block %d of section %d: %w", currentRow, loc.BlockIndex, loc.Section, err)
		}
		if currentRow == loc.RowIndex {
			// Write zero length to mark row as deleted
			if err := binary.Write(newBuf, binary.LittleEndian, uint32(0)); err != nil {
				return fmt.Errorf("failed to write zero length for row %d in block %d of section %d: %w", currentRow, loc.BlockIndex, loc.Section, err)
			}
		} else {
			// Copy row as is
			if err := binary.Write(newBuf, binary.LittleEndian, uint32(len(rowData))); err != nil {
				return fmt.Errorf("failed to write length for row %d in block %d of section %d: %w", currentRow, loc.BlockIndex, loc.Section, err)
			}
			newBuf.Write(rowData)
		}
		currentRow++
	}

	// Compress the modified block
	newData := newBuf.Bytes()
	cNew, err := zstd.Compress(nil, newData)
	if err != nil {
		return fmt.Errorf("failed to compress modified block: %w", err)
	}

	// Write the compressed modified block back
	_, err = file.Seek(int64(blockMeta.FileOffset), io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to block offset %d in section %d: %w", blockMeta.FileOffset, loc.Section, err)
	}
	_, err = file.Write(cNew)
	if err != nil {
		return fmt.Errorf("failed to write modified compressed block at offset %d in section %d: %w", blockMeta.FileOffset, loc.Section, err)
	}

	// Update metadata
	sec.Blocks[loc.BlockIndex].Size = uint32(len(cNew))
	sec.DeletedRows++
	sec.LiveBytes -= loc.Size

	return nil
}

func (b *JSONTable) DeleteRow(loc *benchtop.RowLoc, id []byte) error {
	sec, exists := b.Sections[loc.Section]
	if !exists {
		return fmt.Errorf("section %d not found", loc.Section)
	}

	// Handle in-memory block if BlockIndex points to the current block
	if int(loc.BlockIndex) == len(sec.Blocks) {
		sec.Lock.Lock()
		defer sec.Lock.Unlock()
		if sec.CurrentBlock == nil || int(loc.RowIndex) >= len(sec.CurrentBlock.Rows) {
			return fmt.Errorf("row index %d out of range in current block of section %d", loc.RowIndex, loc.Section)
		}
		// Mark row as deleted by setting its data to an empty slice
		sec.CurrentBlock.Size -= uint32(len(sec.CurrentBlock.Rows[loc.RowIndex]))
		sec.CurrentBlock.Rows[loc.RowIndex] = nil // Mark as deleted
		sec.DeletedRows++
		sec.LiveBytes -= loc.Size
		return nil
	}

	// Handle flushed block
	if int(loc.BlockIndex) >= len(sec.Blocks) {
		return fmt.Errorf("block index %d out of range in section %d", loc.BlockIndex, loc.Section)
	}

	sec.Lock.Lock()
	defer sec.Lock.Unlock()

	// Get block metadata
	blockMeta := sec.Blocks[loc.BlockIndex]

	// Seek to the start of the block
	_, err := sec.File.Seek(int64(blockMeta.FileOffset), io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to block offset %d in section %d: %w", blockMeta.FileOffset, loc.Section, err)
	}

	// Read the entire block
	blockData := make([]byte, blockMeta.Size)
	_, err = io.ReadFull(sec.File, blockData)
	if err != nil {
		return fmt.Errorf("failed to read block data at offset %d in section %d: %w", blockMeta.FileOffset, loc.Section, err)
	}

	// Create a new buffer for the modified block
	newBuf := new(bytes.Buffer)
	it := sec.CurrentBlock.Iterator(blockData)
	currentRow := uint16(0)

	// Rewrite block, skipping or zeroing the deleted row
	for {
		rowData, err := it.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read row %d in block %d of section %d: %w", currentRow, loc.BlockIndex, loc.Section, err)
		}
		if currentRow == loc.RowIndex {
			// Write zero length to mark row as deleted
			if err := binary.Write(newBuf, binary.LittleEndian, uint32(0)); err != nil {
				return fmt.Errorf("failed to write zero length for row %d in block %d of section %d: %w", currentRow, loc.BlockIndex, loc.Section, err)
			}
		} else {
			// Copy row as is
			if err := binary.Write(newBuf, binary.LittleEndian, uint32(len(rowData))); err != nil {
				return fmt.Errorf("failed to write length for row %d in block %d of section %d: %w", currentRow, loc.BlockIndex, loc.Section, err)
			}
			newBuf.Write(rowData)
		}
		currentRow++
	}

	// Write the modified block back to the file
	_, err = sec.File.Seek(int64(blockMeta.FileOffset), io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to block offset %d in section %d: %w", blockMeta.FileOffset, loc.Section, err)
	}
	newData := newBuf.Bytes()
	_, err = sec.File.Write(newData)
	if err != nil {
		return fmt.Errorf("failed to write modified block at offset %d in section %d: %w", blockMeta.FileOffset, loc.Section, err)
	}

	// Update block metadata
	sec.Blocks[loc.BlockIndex].Size = uint32(len(newData))
	sec.DeletedRows++
	sec.LiveBytes -= loc.Size

	return nil
}

// Scan scans through all blocks and sections in the table, applying the filter if provided.
func (b *JSONTable) Scan(loadData bool, filter benchtop.RowFilter) chan any {
	outChan := make(chan any, 100*len(b.Sections))
	var wg sync.WaitGroup
	sem := make(chan struct{}, b.MaxConcurrentSections)

	// Number of workers for parallel row processing within a block
	const rowWorkers = 4

	partitions := make(map[uint8]bool)
	for i := uint8(0); i < uint8(b.NumPartitions); i++ {
		partitions[i] = true
	}

	for pId := range partitions {
		for _, secId := range b.PartitionMap[pId] {
			sec, exists := b.Sections[secId]
			if !exists {
				logrus.Debugf("SECTION: %d does not exist in %#v", secId, b.Sections)
				continue
			}
			wg.Add(1)
			go func(sec *section.Section) {
				defer wg.Done()
				sem <- struct{}{}
				handle := <-sec.FilePool
				defer func() {
					sec.FilePool <- handle
					<-sem
				}()

				// Check if section has any data
				sec.Lock.RLock()
				hasInMemoryData := sec.CurrentBlock != nil && len(sec.CurrentBlock.Rows) > 0
				sec.Lock.RUnlock()
				hasFlushedData := len(sec.Blocks) > 0
				if !hasInMemoryData && !hasFlushedData {
					logrus.Debugf("Skipping empty section %d (%s).", sec.ID, handle.Name())
					return
				}

				// Process in-memory block first
				if hasInMemoryData {
					var rowWG sync.WaitGroup
					rowSem := make(chan struct{}, rowWorkers)
					sec.Lock.RLock()
					for _, rowData := range sec.CurrentBlock.Rows {
						if len(rowData) == 0 {
							continue // Skip deleted or empty rows
						}
						rowWG.Add(1)
						rowSem <- struct{}{}
						go func(data []byte) {
							defer rowWG.Done()
							defer func() { <-rowSem }()
							processErr := b.processJSONRowData(data, loadData, filter, outChan)
							if processErr != nil {
								logrus.Debugf("Skipping malformed in-memory row at section %d: %v", sec.ID, processErr)
							}
						}(rowData)
					}
					sec.Lock.RUnlock()
					rowWG.Wait()
				}

				// Process flushed blocks from disk
				if hasFlushedData {
					var rowWG sync.WaitGroup
					rowSem := make(chan struct{}, rowWorkers)
					for _, blk := range sec.Blocks {
						_, err := handle.Seek(int64(blk.FileOffset), io.SeekStart)
						if err != nil {
							logrus.Errorf("Failed to seek to block offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
							continue
						}
						blockData := make([]byte, blk.Size)
						_, err = io.ReadFull(handle, blockData)
						if err != nil {
							logrus.Errorf("Failed to read block data at offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
							continue
						}
						it := block.NewMemBlock().Iterator(blockData)
						for {
							rowData, err := it.Next()
							if err != nil {
								if err != io.EOF {
									logrus.Errorf("Error reading row in block at section %d, offset %d: %v", sec.ID, blk.FileOffset, err)
								}
								break
							}
							if len(rowData) == 0 {
								continue // Skip deleted or empty rows
							}
							rowWG.Add(1)
							rowSem <- struct{}{}
							go func(data []byte) {
								defer rowWG.Done()
								defer func() { <-rowSem }()
								processErr := b.processJSONRowData(data, loadData, filter, outChan)
								if processErr != nil {
									log.Debugf("Skipping malformed row in block at section %d, offset %d: %v", sec.ID, blk.FileOffset, processErr)
								}
							}(rowData)
						}
					}
					rowWG.Wait()
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
// Updated processJSONRowData without decompression
func (b *JSONTable) processJSONRowData(
	rowData []byte,
	loadData bool,
	filter benchtop.RowFilter,
	outChan chan any,
) error {
	var val any

	newData := rowData // No decompression, as rows are uncompressed within block

	if loadData || filter != nil && !filter.IsNoOp() {
		var m RowData
		err := sonic.ConfigFastest.Unmarshal(newData, &m)
		if err != nil {
			return err
		}
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
		inputChan <- benchtop.Index{Key: []byte(key), Loc: benchtop.RowLoc{BlockIndex: uint16(newOffset), Size: bSize}}

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
