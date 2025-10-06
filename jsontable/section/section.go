package section

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/DataDog/zstd"
	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/block"
)

const (
	MAX_BLOCK_SIZE uint32 = 52_428_800 // 50 MB
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

/*
func (s *Section) WriteJsonEntryToSection(payload []byte) (*benchtop.RowLoc, error) {
	// Validate payload is valid JSON (for debugging)
	var temp any
	if err := sonic.ConfigFastest.Unmarshal(payload, &temp); err != nil {
		return nil, fmt.Errorf("input payload is not valid JSON: %w", err)
	}

	// Compress payload with explicit compression level
	cPayload, err := zstd.Compress(nil, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to compress payload: %w", err)
	}
	compressedLen := uint32(len(cPayload))

	// Build header
	header := make([]byte, benchtop.ROW_HSIZE)                                              // ROW_HSIZE == 8
	binary.LittleEndian.PutUint32(header[:4], s.LiveBytes+compressedLen+benchtop.ROW_HSIZE) // Next offset
	binary.LittleEndian.PutUint32(header[4:], compressedLen)                                // Compressed data length
	cPayload = append(header, cPayload...)

	// Write to file
	_, err = s.File.Write(cPayload)
	if err != nil {
		return nil, fmt.Errorf("failed to write payload: %w", err)
	}

	// Update LiveBytes and return
	s.LiveBytes += compressedLen + benchtop.ROW_HSIZE
	return &benchtop.RowLoc{
		Section: s.ID,
		Offset:  s.LiveBytes - (compressedLen + benchtop.ROW_HSIZE), // Start of this row
		Size:    compressedLen,                                      // Compressed data size only
	}, nil
}
*/
