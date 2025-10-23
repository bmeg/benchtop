package section

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/DataDog/zstd"
	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/block"
	"github.com/caarlos0/log"
)

const (
	MAX_BLOCK_SIZE uint32 = 10_485_760 //52_428_800 // 50 MB
)

type BlockMeta struct {
	FileOffset uint32 // Offset in the section file where the block starts
	Size       uint32 // Compressed block size
	RowCount   uint16 // Number of rows in the block
}

type Section struct {
	ID           uint16
	PartitionID  uint8
	Path         string
	File         *os.File
	Writer       *zstd.Writer
	FilePool     chan *os.File
	Lock         sync.RWMutex
	TotalRows    uint32
	LiveBytes    uint32
	Active       bool
	CurrentBlock *block.MemBlock
	Blocks       []BlockMeta
	DeletedRows  int
	cachedBlocks map[int][]byte
}

// Update AppendRow to check size before appending
func (s *Section) AppendRow(row []byte) (*benchtop.RowLoc, error) {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	/*  Validate JSON
	var tmp any
	if err := sonic.ConfigFastest.Unmarshal(row, &tmp); err != nil {
		return nil, fmt.Errorf("invalid JSON row: %w", err)
	}*/

	// Check if adding this row would exceed MaxBlockSize (approximate serialized size: len(row) + 4)
	if s.CurrentBlock.Size+uint32(len(row))+4 > MAX_BLOCK_SIZE {
		if flushErr := s.flushBlock(); flushErr != nil {
			return nil, flushErr
		}
	}

	s.TotalRows++
	// BlockIndex points to the current (potentially future) block index
	return &benchtop.RowLoc{
		Section:    s.ID,
		BlockIndex: uint16(len(s.Blocks)),
		RowIndex:   s.CurrentBlock.Append(row),
		Size:       uint32(len(row)),
	}, nil
}

// Update flushBlock to track LiveBytes accurately
func (s *Section) flushBlock() error {
	if s.CurrentBlock == nil || len(s.CurrentBlock.Rows) == 0 {
		return nil
	}

	data, err := s.CurrentBlock.Finalize()
	if err != nil {
		return fmt.Errorf("failed to finalize block: %w", err)
	}

	// Compress the entire block
	cData, err := zstd.Compress(nil, data)
	if err != nil {
		return fmt.Errorf("failed to compress block: %w", err)
	}

	offset, err := s.File.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get file offset: %w", err)
	}

	n, err := s.File.Write(cData)
	if err != nil {
		return fmt.Errorf("failed to write compressed block: %w", err)
	}

	// Track block metadata with compressed size
	s.Blocks = append(s.Blocks, BlockMeta{
		FileOffset: uint32(offset),
		Size:       uint32(n),
		RowCount:   uint16(len(s.CurrentBlock.Rows)),
	})

	s.LiveBytes += uint32(n)

	s.CurrentBlock = block.NewMemBlock()
	return nil
}

// BlockIterator returns an iterator over the current in-memory block
func (s *Section) BlockIterator(blockBytes []byte) *block.MemBlockIterator {
	return block.NewMemBlock().Iterator(blockBytes)
}

// getDecompressedBlock retrieves or loads and caches a decompressed block
func (sec *Section) GetDecompressedBlock(blockIndex int, section uint16) ([]byte, error) {
	start := time.Now()
	cacheHit := false
	defer func() {
		log.Debugf("getDecompressedBlock(section=%d, block=%d): total_duration=%dµs, cache_hit=%t", section, blockIndex, time.Since(start).Microseconds(), cacheHit)
	}()

	decompressed, ok := sec.cachedBlocks[blockIndex]
	if ok {
		cacheHit = true
		return decompressed, nil
	}

	// Double-check under write lock
	decompressed, ok = sec.cachedBlocks[blockIndex]
	if ok {
		cacheHit = true
		return decompressed, nil
	}

	tFile := time.Now()
	file := <-sec.FilePool
	defer func() { sec.FilePool <- file }()
	log.Debugf("getDecompressedBlock(section=%d, block=%d): file_acquire_duration=%dµs", section, blockIndex, time.Since(tFile).Microseconds())

	blockMeta := sec.Blocks[blockIndex]

	tSeek := time.Now()
	if _, err := file.Seek(int64(blockMeta.FileOffset), io.SeekStart); err != nil {
		log.Debugf("getDecompressedBlock(section=%d, block=%d): seek_duration=%dµs", section, blockIndex, time.Since(tSeek).Microseconds())
		return nil, fmt.Errorf("failed to seek to block offset %d in section %d: %w", blockMeta.FileOffset, section, err)
	}
	log.Debugf("getDecompressedBlock(section=%d, block=%d): seek_duration=%dµs", section, blockIndex, time.Since(tSeek).Microseconds())

	tRead := time.Now()
	cData := make([]byte, blockMeta.Size)
	if _, err := io.ReadFull(file, cData); err != nil {
		log.Debugf("getDecompressedBlock(section=%d, block=%d): read_duration=%dµs", section, blockIndex, time.Since(tRead).Microseconds())
		return nil, fmt.Errorf("failed to read compressed block data at offset %d in section %d: %w", blockMeta.FileOffset, section, err)
	}
	log.Debugf("getDecompressedBlock(section=%d, block=%d): read_duration=%dµs", section, blockIndex, time.Since(tRead).Microseconds())

	tDecompress := time.Now()
	decompressed, err := zstd.Decompress(nil, cData)
	if err != nil {
		log.Debugf("getDecompressedBlock(section=%d, block=%d): decompress_duration=%dµs", section, blockIndex, time.Since(tDecompress).Microseconds())
		return nil, fmt.Errorf("failed to decompress block at offset %d in section %d: %w", blockMeta.FileOffset, section, err)
	}
	log.Debugf("getDecompressedBlock(section=%d, block=%d): decompress_duration=%dµs", section, blockIndex, time.Since(tDecompress).Microseconds())

	// Initialize cache if nil
	if sec.cachedBlocks == nil {
		sec.cachedBlocks = make(map[int][]byte)
	}
	sec.cachedBlocks[blockIndex] = decompressed

	// Simple eviction: limit to 100 blocks
	const maxCachedBlocks = 100
	if len(sec.cachedBlocks) > maxCachedBlocks {
		// Evict the smallest index
		minIndex := blockIndex
		for idx := range sec.cachedBlocks {
			if idx < minIndex {
				minIndex = idx
			}
		}
		delete(sec.cachedBlocks, minIndex)
	}

	return decompressed, nil
}
