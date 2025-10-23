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
	"time"

	"github.com/DataDog/zstd"
	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/block"
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
	start := time.Now()
	defer func() {
		log.Debugf("GetRow(section=%d, block=%d, row=%d): total_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(start).Microseconds())
	}()

	//tSection := time.Now()
	sec, exists := b.Sections[loc.Section]
	if !exists {
		//log.Debugf("GetRow(section=%d, block=%d, row=%d): section_lookup_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(tSection).Microseconds())
		return nil, fmt.Errorf("section %d not found", loc.Section)
	}
	//log.Debugf("GetRow(section=%d, block=%d, row=%d): section_lookup_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(tSection).Microseconds())

	// sec.Lock.RLock()
	// defer sec.Lock.RUnlock()

	//tBoundsCheck := time.Now()
	if int(loc.BlockIndex) == len(sec.Blocks) && sec.CurrentBlock != nil {
		if int(loc.RowIndex) >= len(sec.CurrentBlock.Rows) {
			//log.Debugf("GetRow(section=%d, block=current, row=%d): bounds_check_duration=%dµs", loc.Section, loc.RowIndex, time.Since(tBoundsCheck).Microseconds())
			return nil, fmt.Errorf("row index %d out of range in current block of section %d", loc.RowIndex, loc.Section)
		}
		rowData := sec.CurrentBlock.Rows[loc.RowIndex]
		if len(rowData) == 0 {
			//log.Debugf("GetRow(section=%d, block=current, row=%d): bounds_check_duration=%dµs", loc.Section, loc.RowIndex, time.Since(tBoundsCheck).Microseconds())
			return nil, fmt.Errorf("row %d in current block of section %d is deleted or invalid", loc.RowIndex, loc.Section)
		}
		if uint32(len(rowData)) != loc.Size {
			//log.Debugf("GetRow(section=%d, block=current, row=%d): bounds_check_duration=%dµs", loc.Section, loc.RowIndex, time.Since(tBoundsCheck).Microseconds())
			return nil, fmt.Errorf("row size mismatch in current block of section %d: expected %d, got %d", loc.Section, loc.Size, len(rowData))
		}
		//log.Debugf("GetRow(section=%d, block=current, row=%d): bounds_check_duration=%dµs", loc.Section, loc.RowIndex, time.Since(tBoundsCheck).Microseconds())
		//tUnmarshal := time.Now()
		var m RowData
		err := sonic.ConfigFastest.Unmarshal(rowData, &m)
		if err != nil {
			//log.Debugf("GetRow(section=%d, block=current, row=%d): unmarshal_duration=%dµs", loc.Section, loc.RowIndex, time.Since(tUnmarshal).Microseconds())
			return nil, fmt.Errorf("failed to decode JSON row %d in current block of section %d: %w", loc.RowIndex, loc.Section, err)
		}
		//log.Debugf("GetRow(section=%d, block=current, row=%d): unmarshal_duration=%dµs", loc.Section, loc.RowIndex, time.Since(tUnmarshal).Microseconds())
		//tUnpack := time.Now()
		out, err := b.unpackData(&m)
		if err != nil {
			//log.Debugf("GetRow(section=%d, block=current, row=%d): unpack_duration=%dµs", loc.Section, loc.RowIndex, time.Since(tUnpack).Microseconds())
			return nil, fmt.Errorf("failed to unpack data for row %d in current block of section %d: %w", loc.RowIndex, loc.Section, err)
		}
		//log.Debugf("GetRow(section=%d, block=current, row=%d): unpack_duration=%dµs", loc.Section, loc.RowIndex, time.Since(tUnpack).Microseconds())
		return out, nil
	}
	//log.Debugf("GetRow(section=%d, block=%d, row=%d): bounds_check_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(tBoundsCheck).Microseconds())

	if int(loc.BlockIndex) >= len(sec.Blocks) {
		return nil, fmt.Errorf("block index %d out of range in section %d", loc.BlockIndex, loc.Section)
	}

	//tDecompress := time.Now()
	decompressed, err := sec.GetDecompressedBlock(int(loc.BlockIndex), loc.Section)
	if err != nil {
		//log.Debugf("GetRow(section=%d, block=%d, row=%d): decompress_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(tDecompress).Microseconds())
		return nil, err
	}
	//log.Debugf("GetRow(section=%d, block=%d, row=%d): decompress_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(tDecompress).Microseconds())

	//tIterator := time.Now()
	it := block.NewMemBlock().Iterator(decompressed)
	//log.Debugf("GetRow(section=%d, block=%d, row=%d): iterator_create_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(tIterator).Microseconds())
	//tLookup := time.Now()
	rowData, err := it.Lookup(loc.RowIndex)
	if err != nil {
		//log.Debugf("GetRow(section=%d, block=%d, row=%d): iterator_lookup_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(tLookup).Microseconds())
		return nil, fmt.Errorf("failed to lookup row %d in block %d of section %d: %w", loc.RowIndex, loc.BlockIndex, loc.Section, err)
	}
	//log.Debugf("GetRow(section=%d, block=%d, row=%d): iterator_lookup_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(tLookup).Microseconds())

	//tSizeCheck := time.Now()
	if uint32(len(rowData)) != loc.Size {
		//log.Debugf("GetRow(section=%d, block=%d, row=%d): size_check_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(tSizeCheck).Microseconds())
		return nil, fmt.Errorf("row size mismatch in block %d of section %d: expected %d, got %d", loc.BlockIndex, loc.Section, loc.Size, len(rowData))
	}
	//log.Debugf("GetRow(section=%d, block=%d, row=%d): size_check_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(tSizeCheck).Microseconds())

	//tUnmarshal := time.Now()
	var m RowData
	err = sonic.ConfigFastest.Unmarshal(rowData, &m)
	if err != nil {
		//log.Debugf("GetRow(section=%d, block=%d, row=%d): unmarshal_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(tUnmarshal).Microseconds())
		return nil, fmt.Errorf("failed to decode JSON row %d in block %d of section %d: %w", loc.RowIndex, loc.BlockIndex, loc.Section, err)
	}
	//log.Debugf("GetRow(section=%d, block=%d, row=%d): unmarshal_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(tUnmarshal).Microseconds())

	//tUnpack := time.Now()
	out, err := b.unpackData(&m)
	if err != nil {
		//log.Debugf("GetRow(section=%d, block=%d, row=%d): unpack_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(tUnpack).Microseconds())
		return nil, fmt.Errorf("failed to unpack data for row %d in block %d of section %d: %w", loc.RowIndex, loc.BlockIndex, loc.Section, err)
	}
	//log.Debugf("GetRow(section=%d, block=%d, row=%d): unpack_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(tUnpack).Microseconds())

	//tReturn := time.Now()
	//log.Debugf("GetRow(section=%d, block=%d, row=%d): return_duration=%dµs", loc.Section, loc.BlockIndex, loc.RowIndex, time.Since(tReturn).Microseconds())
	return out, nil
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

/*
// Scan scans through all blocks and sections, applying the filter if provided.
// Optimized for speed using streaming row processing via MemBlockIterator,
// lazy unmarshaling, and sequential fallback for low section counts.
func (b *JSONTable) Scan(loadData bool, filter benchtop.RowFilter) chan any {
	log.Println("ENTERING OPTIMIZED HYBRID SCAN FUNCTION")
	// Buffer size for output channel (tune based on consumer speed).
	outChan := make(chan any, 100*len(b.Sections))
	var wg sync.WaitGroup

	// Fallback to sequential if few sections to avoid goroutine overhead.
	// Tune threshold based on profiling (e.g., 8 for 4-core CPU).
	if len(b.Sections) < 8 {
		b.scanSequential(loadData, filter, outChan)
		return outChan
	}

	// Concurrent mode: Semaphore limits goroutines (tune to CPU cores / 2).
	sem := make(chan struct{}, runtime.NumCPU()/2)

	partitions := make(map[uint8]bool)
	for i := uint8(0); i < uint8(b.NumPartitions); i++ {
		partitions[i] = true
	}

	for pId := range partitions {
		for _, secId := range b.PartitionMap[pId] {
			sec, exists := b.Sections[secId]
			if !exists {
				log.Debugf("SECTION: %d does not exist in %#v", secId, b.Sections)
				continue
			}
			wg.Add(1)
			go func(sec *section.Section) {
				defer wg.Done()
				sem <- struct{}{} // Acquire semaphore slot

				// Get file handle from pool.
				handle := <-sec.FilePool
				defer func() {
					sec.FilePool <- handle
					<-sem
				}()

				// Check if section has data.
				sec.Lock.RLock()
				hasInMemoryData := sec.CurrentBlock != nil && len(sec.CurrentBlock.Rows) > 0
				hasFlushedData := len(sec.Blocks) > 0
				sec.Lock.RUnlock()

				if !hasInMemoryData && !hasFlushedData {
					log.Debugf("Skipping empty section %d (%s).", sec.ID, handle.Name())
					return
				}

				// Process in-memory block.
				if hasInMemoryData {
					sec.Lock.RLock()
					for _, rowData := range sec.CurrentBlock.Rows {
						if len(rowData) == 0 {
							continue // Skip deleted/empty rows.
						}
						if err := b.processJSONRowData(rowData, loadData, filter, outChan); err != nil {
							log.Debugf("Skipping malformed in-memory row at section %d: %v", sec.ID, err)
						}
					}
					sec.Lock.RUnlock()
				}

				// Process flushed blocks with streaming.
				if hasFlushedData {
					for _, blk := range sec.Blocks {
						// Read compressed block.
						if _, err := handle.Seek(int64(blk.FileOffset), io.SeekStart); err != nil {
							log.Errorf("Failed to seek to block offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
							continue
						}
						blockData := make([]byte, blk.Size)
						if _, err := io.ReadFull(handle, blockData); err != nil {
							log.Errorf("Failed to read block data at offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
							continue
						}

						// Decompress using pooled buffer.
						buf := decompressPool.Get().([]byte)[:0]
						decompressed, err := zstd.Decompress(buf, blockData)
						if err != nil {
							log.Errorf("Failed to decompress block at offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
							decompressPool.Put(decompressed[:0])
							continue
						}

						// Stream rows with iterator.
						it := block.NewMemBlock().Iterator(decompressed)
						if it == nil {
							log.Errorf("Corrupt block at offset %d in section %d: invalid data", blk.FileOffset, sec.ID)
							decompressPool.Put(decompressed[:0])
							continue
						}

						for {
							rowBytes, err := it.Next()
							if err == io.EOF {
								break
							}
							if err != nil {
								log.Debugf("Skipping malformed row in block at offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
								continue
							}
							if len(rowBytes) == 0 {
								continue // Skip empty/deleted.
							}

							if err := b.processJSONRowData(rowBytes, loadData, filter, outChan); err != nil {
								log.Debugf("Skipping malformed row in block at offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
							}
						}
						decompressPool.Put(decompressed[:0])
					}
				}
			}(sec)
		}
	}

	// Close output channel when done.
	go func() {
		wg.Wait()
		close(outChan)
	}()
	return outChan
}

// scanSequential is a fallback for low section counts to avoid goroutine overhead.
func (b *JSONTable) scanSequential(loadData bool, filter benchtop.RowFilter, outChan chan any) {
	for pId := uint8(0); pId < uint8(b.NumPartitions); pId++ {
		for _, secId := range b.PartitionMap[pId] {
			sec, exists := b.Sections[secId]
			if !exists {
				log.Debugf("SECTION: %d does not exist in %#v", secId, b.Sections)
				continue
			}

			// Get file handle.
			handle := <-sec.FilePool
			defer func() {
				sec.FilePool <- handle
			}()

			// Check if section has data.
			sec.Lock.RLock()
			hasInMemoryData := sec.CurrentBlock != nil && len(sec.CurrentBlock.Rows) > 0
			hasFlushedData := len(sec.Blocks) > 0
			sec.Lock.RUnlock()

			if !hasInMemoryData && !hasFlushedData {
				log.Debugf("Skipping empty section %d (%s).", sec.ID, handle.Name())
				continue
			}

			// Process in-memory block.
			if hasInMemoryData {
				sec.Lock.RLock()
				for _, rowData := range sec.CurrentBlock.Rows {
					if len(rowData) == 0 {
						continue
					}
					if err := b.processJSONRowData(rowData, loadData, filter, outChan); err != nil {
						log.Debugf("Skipping malformed in-memory row at section %d: %v", sec.ID, err)
					}
				}
				sec.Lock.RUnlock()
			}

			// Process flushed blocks.
			if hasFlushedData {
				for _, blk := range sec.Blocks {
					if _, err := handle.Seek(int64(blk.FileOffset), io.SeekStart); err != nil {
						log.Errorf("Failed to seek to block offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
						continue
					}
					blockData := make([]byte, blk.Size)
					if _, err := io.ReadFull(handle, blockData); err != nil {
						log.Errorf("Failed to read block data at offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
						continue
					}

					buf := decompressPool.Get().([]byte)[:0]
					decompressed, err := zstd.Decompress(buf, blockData)
					if err != nil {
						log.Errorf("Failed to decompress block at offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
						decompressPool.Put(decompressed[:0])
						continue
					}

					it := block.NewMemBlock().Iterator(decompressed)
					if it == nil {
						log.Errorf("Corrupt block at offset %d in section %d: invalid data", blk.FileOffset, sec.ID)
						decompressPool.Put(decompressed[:0])
						continue
					}

					for {
						rowBytes, err := it.Next()
						if err == io.EOF {
							break
						}
						if err != nil {
							log.Debugf("Skipping malformed row in block at offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
							continue
						}
						if len(rowBytes) == 0 {
							continue
						}

						if err := b.processJSONRowData(rowBytes, loadData, filter, outChan); err != nil {
							log.Debugf("Skipping malformed row in block at offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
						}
					}
					decompressPool.Put(decompressed[:0])
				}
			}
		}
	}
	close(outChan)
}

// processJSONRowData parses row bytes and sends to outChan if it matches the filter.
// Optimized for speed with lazy key parsing when possible.
func (b *JSONTable) processJSONRowData(rowData []byte, loadData bool, filter benchtop.RowFilter, outChan chan any) error {
	if !loadData && (filter == nil || filter.IsNoOp()) {
		// Fast path: Parse only the key field.
		var partial struct {
			Key string `json:"key"`
		}
		if err := sonic.ConfigFastest.Unmarshal(rowData, &partial); err != nil {
			return fmt.Errorf("failed to unmarshal key: %w", err)
		}
		outChan <- partial.Key
		return nil
	}

	// Full unmarshal for filtering or loading.
	var m RowData
	if err := sonic.ConfigFastest.Unmarshal(rowData, &m); err != nil {
		return fmt.Errorf("failed to unmarshal row: %w", err)
	}

	var val any
	var err error
	if loadData || (filter != nil && !filter.IsNoOp()) {
		// Fixed bug: Use the unmarshaled m instead of a fresh RowData.
		val, err = b.unpackData(true, true, &m)
		if err != nil {
			return fmt.Errorf("failed to unpack data: %w", err)
		}
	} else {
		val = m.Key
	}

	if filter == nil || filter.IsNoOp() || filter.Matches(val) {
		outChan <- val
	}
	return nil
}

*/

// ScanIDs scans through all blocks and sections in the table, returning only row IDs as strings.
// Optimized to avoid full unmarshaling and type casting for minimal CPU and memory usage.

// ScanIDs scans through all blocks and sections in the table, returning only row IDs as strings.
// Optimized to avoid unmarshaling and type casting for minimal CPU and memory usage.
func (b *JSONTable) ScanIDs(filter benchtop.RowFilter) chan string {
	fmt.Println("ENTERING OPTIMIZED SCAN_IDS FUNCTION")
	outChan := make(chan string, 100*len(b.Sections))
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
				log.Debugf("SECTION: %d does not exist in %#v", secId, b.Sections)
				continue
			}
			wg.Add(1)
			go func(sec *section.Section) {
				defer wg.Done()
				sem <- struct{}{} // Acquire semaphore slot

				// Get file handle from the pool
				handle := <-sec.FilePool

				defer func() {
					// Release file handle back to the pool
					sec.FilePool <- handle
					// Release semaphore slot
					<-sem
				}()

				// Check if section has any data
				sec.Lock.RLock()
				hasInMemoryData := sec.CurrentBlock != nil && len(sec.CurrentBlock.Rows) > 0
				hasFlushedData := len(sec.Blocks) > 0
				sec.Lock.RUnlock()

				if !hasInMemoryData && !hasFlushedData {
					log.Debugf("Skipping empty section %d (%s).", sec.ID, handle.Name())
					return
				}

				// --- 1. Process in-memory block (Sequential) ---
				if hasInMemoryData {
					sec.Lock.RLock()
					for _, rowData := range sec.CurrentBlock.Rows {
						if len(rowData) == 0 {
							continue // Skip deleted or empty rows
						}
						if err := b.processJSONRowDataForIDs(rowData, filter, outChan); err != nil {
							log.Debugf("Skipping malformed in-memory row at section %d: %v", sec.ID, err)
						}
					}
					sec.Lock.RUnlock()
				}

				// --- 2. Process flushed blocks from disk (Sequential Block Processing) ---
				if hasFlushedData {
					for _, blk := range sec.Blocks {
						// Read compressed data from disk
						_, err := handle.Seek(int64(blk.FileOffset), io.SeekStart)
						if err != nil {
							log.Errorf("Failed to seek to block offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
							continue
						}
						blockData := make([]byte, blk.Size)
						_, err = io.ReadFull(handle, blockData)
						if err != nil {
							log.Errorf("Failed to read block data at offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
							continue
						}

						// Decompress the entire block
						decompressed, err := zstd.Decompress(nil, blockData)
						if err != nil {
							log.Errorf("Failed to decompress block data at offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
							continue
						}

						// Identify JSON array payload (strip binary footer)
						if len(decompressed) < 2 {
							log.Errorf("Corrupt block: too short for section %d offset %d", sec.ID, blk.FileOffset)
							continue
						}
						rowCount := binary.LittleEndian.Uint16(decompressed[len(decompressed)-2:])
						indexSize := 2 + int(rowCount)*4
						jsonEnd := len(decompressed) - indexSize
						if jsonEnd <= 0 {
							log.Errorf("Corrupt block (bad offset table) section %d offset %d", sec.ID, blk.FileOffset)
							continue
						}

						jsonPayload := decompressed[:jsonEnd]

						// Fast path: Extract IDs directly if no filter

						// Unmarshal for filtering
						var rows []RowData
						if err := sonic.ConfigFastest.Unmarshal(jsonPayload, &rows); err != nil {
							log.Errorf("Failed to unmarshal full block array in section %d: %v", sec.ID, err)
							continue
						}
						for _, rowData := range rows {
							if filter == nil || filter.IsNoOp() || filter.Matches(rowData) {
								outChan <- rowData.Key
							}
						}

					}
				}
			}(sec)
		}
	}

	// Wait for all section workers to finish and close the output channel
	go func() {
		wg.Wait()
		close(outChan)
	}()
	return outChan
}

// ScanData scans through all blocks and sections in the table, returning full map[string]any data.
// Processes rows sequentially within blocks and applies filters as needed.
func (b *JSONTable) ScanRows(filter benchtop.RowFilter) chan map[string]any {
	fmt.Println("ENTERING OPTIMIZED SCAN_DATA FUNCTION")
	outChan := make(chan map[string]any, 100*len(b.Sections))
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
				log.Debugf("SECTION: %d does not exist in %#v", secId, b.Sections)
				continue
			}
			wg.Add(1)
			go func(sec *section.Section) {
				defer wg.Done()
				sem <- struct{}{} // Acquire semaphore slot

				// Get file handle from the pool
				handle := <-sec.FilePool

				defer func() {
					// Release file handle back to the pool
					sec.FilePool <- handle
					// Release semaphore slot
					<-sem
				}()

				// Check if section has any data
				sec.Lock.RLock()
				hasInMemoryData := sec.CurrentBlock != nil && len(sec.CurrentBlock.Rows) > 0
				hasFlushedData := len(sec.Blocks) > 0
				sec.Lock.RUnlock()

				if !hasInMemoryData && !hasFlushedData {
					log.Debugf("Skipping empty section %d (%s).", sec.ID, handle.Name())
					return
				}

				// --- 1. Process in-memory block (Sequential) ---
				if hasInMemoryData {
					sec.Lock.RLock()
					for _, rowData := range sec.CurrentBlock.Rows {
						if len(rowData) == 0 {
							continue // Skip deleted or empty rows
						}
						if err := b.processJSONRowDataForData(rowData, filter, outChan); err != nil {
							log.Debugf("Skipping malformed in-memory row at section %d: %v", sec.ID, err)
						}
					}
					sec.Lock.RUnlock()
				}

				// --- 2. Process flushed blocks from disk (Sequential Block Processing) ---
				if hasFlushedData {
					for _, blk := range sec.Blocks {
						// Read compressed data from disk
						_, err := handle.Seek(int64(blk.FileOffset), io.SeekStart)
						if err != nil {
							log.Errorf("Failed to seek to block offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
							continue
						}
						blockData := make([]byte, blk.Size)
						_, err = io.ReadFull(handle, blockData)
						if err != nil {
							log.Errorf("Failed to read block data at offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
							continue
						}

						// Decompress the entire block
						decompressed, err := zstd.Decompress(nil, blockData)
						if err != nil {
							log.Errorf("Failed to decompress block data at offset %d in section %d: %v", blk.FileOffset, sec.ID, err)
							continue
						}

						// Identify JSON array payload (strip binary footer)
						if len(decompressed) < 2 {
							log.Errorf("Corrupt block: too short for section %d offset %d", sec.ID, blk.FileOffset)
							continue
						}
						rowCount := binary.LittleEndian.Uint16(decompressed[len(decompressed)-2:])
						indexSize := 2 + int(rowCount)*4
						jsonEnd := len(decompressed) - indexSize
						if jsonEnd <= 0 {
							log.Errorf("Corrupt block (bad offset table) section %d offset %d", sec.ID, blk.FileOffset)
							continue
						}

						jsonPayload := decompressed[:jsonEnd]

						fmt.Println("PAYLOAD > 0", len(jsonPayload))
						// Unmarshal full JSON array
						var rows []RowData
						if err := sonic.ConfigFastest.Unmarshal(jsonPayload, &rows); err != nil {
							log.Errorf("Failed to unmarshal full block array in section %d: %v", sec.ID, err)
							continue
						}
						for _, rowData := range rows {
							val, err := b.unpackData(&rowData)
							if err != nil {
								log.Errorf("Failed to unpack row data in section %d: %v", sec.ID, err)
								continue
							}
							if filter == nil || filter.IsNoOp() || (!filter.IsNoOp() && filter.Matches(val)) {
								fmt.Println("WE HERE!!!")
								outChan <- val
							}
						}
					}
				}
			}(sec)
		}
	}

	// Wait for all section workers to finish and close the output channel
	go func() {
		wg.Wait()
		close(outChan)
	}()
	return outChan
}

// processJSONRowDataForIDs processes a single row for ScanIDs, extracting the ID directly or applying a filter.
func (b *JSONTable) processJSONRowDataForIDs(rowData []byte, filter benchtop.RowFilter, outChan chan string) error {
	if filter != nil && !filter.IsNoOp() {
		var m RowData
		err := sonic.ConfigFastest.Unmarshal(rowData, &m)
		if err != nil {
			return err
		}
		if filter.Matches(m.Data) {
			outChan <- m.Key
		}
		return nil
	}

	// Fast path: Extract ID directly without full unmarshaling
	node, err := sonic.Get(rowData, "1")
	if err != nil {
		log.Errorf("Error accessing JSON path for row data %s: %v", string(rowData), err)
		return err
	}
	id, err := node.String()
	if err != nil {
		log.Errorf("Error unmarshaling node to string: %v", err)
		return err
	}
	outChan <- id
	return nil
}

// processJSONRowDataForData processes a single row for ScanData, unmarshaling and unpacking to map[string]any.
func (b *JSONTable) processJSONRowDataForData(rowData []byte, filter benchtop.RowFilter, outChan chan map[string]any) error {
	var m RowData
	err := sonic.ConfigFastest.Unmarshal(rowData, &m)
	if err != nil {
		return err
	}
	m.Data["_id"] = m.Key
	if filter == nil || filter.IsNoOp() || (!filter.IsNoOp() && filter.Matches(m.Data)) {
		outChan <- m.Data
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
