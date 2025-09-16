package section

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/DataDog/zstd"
	"github.com/bmeg/benchtop"
	"github.com/bytedance/sonic"
)

// Section represents a physical file within a partition
type Section struct {
	ID          uint16 // Global unique section ID (for RowLoc)
	PartitionID uint8  // Partition this section belongs to
	Path        string // File path (e.g., table.data.partition0.section1)
	File        *os.File
	Writer      *zstd.Writer  // Main file handle for writes
	FilePool    chan *os.File // Pool for read/write access
	Lock        sync.RWMutex  // Per-section lock
	TotalRows   uint32        // Total rows (live + deleted)
	DeletedRows uint32        // Deleted rows (for compaction trigger)
	LiveBytes   uint32        // Live data size (bytes)
	Active      bool          // True unless compacted/merged
}

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

func (w *Section) GetCompressedFileOffset() (uint32, error) {
	// You need to flush first to ensure all data is written to the file.
	if err := w.Writer.Flush(); err != nil { // Flush ensures data is written to w.file
		return 0, fmt.Errorf("failed to flush zstd writer before getting file offset: %w", err)
	}
	// Get the current position of the underlying file.
	pos, err := w.File.Seek(0, io.SeekCurrent) // Use SeekCurrent
	if err != nil {
		return 0, fmt.Errorf("failed to get current file offset: %w", err)
	}
	return uint32(pos), nil
}
