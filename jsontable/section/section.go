package section

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/DataDog/zstd"
	"github.com/bmeg/benchtop"
	"github.com/edsrzf/mmap-go"
)

const (
	INITIAL_SECTION_SIZE  = 1 << 20          // 1 MB
	GROWTH_INCREMENT_SIZE = 1 << 24          // 16 MB
	MAX_SECTION_SIZE      = 65 * 1024 * 1024 // 65MB
)

// Section represents a physical file within a partition
type Section struct {
	ID          uint16 // Global unique section ID (for RowLoc)
	PartitionID uint8  // Partition this section belongs to
	Path        string // File path (e.g., table.data.partition0.section1)
	File        *os.File
	FilePool    chan *os.File // Pool for read/write access
	Lock        sync.RWMutex  // Per-section lock
	TotalRows   uint32        // Total rows (live + deleted)
	DeletedRows uint32        // Deleted rows (for compaction trigger)
	LiveBytes   uint32        // Live data size (bytes)
	Active      bool          // True unless compacted/merged

	MMap            mmap.MMap // cached read-only mmap
	MMapMode        int       // mmap.RDWR or mmap.RDONLY
	CompressScratch []byte
}

func (s *Section) WriteJsonEntryToSection(payload []byte) (*benchtop.RowLoc, error) {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	cPayload, err := zstd.Compress(s.CompressScratch[:0], payload)
	if err != nil {
		return nil, fmt.Errorf("compress failed: %w", err)
	}

	compressedLen := uint32(len(cPayload))
	writeEnd := s.LiveBytes + benchtop.ROW_HSIZE + compressedLen

	// Check if write is outside the CURRENT mapped region
	if writeEnd > uint32(len(s.MMap)) {
		currentSize := int64(len(s.MMap))
		requiredSize := int64(writeEnd)

		newSize := requiredSize
		if newSize%GROWTH_INCREMENT_SIZE != 0 {
			newSize = (requiredSize/GROWTH_INCREMENT_SIZE + 1) * GROWTH_INCREMENT_SIZE
		}
		if newSize > MAX_SECTION_SIZE {
			newSize = MAX_SECTION_SIZE
		}
		if newSize > currentSize {
			if err := s.GrowAndRemap(newSize); err != nil {
				return nil, fmt.Errorf("failed to grow section to %d: %w", newSize, err)
			}
		} else {
			// This should not happen if logic is correct, but safe to check
			return nil, fmt.Errorf("mmap too small even after considering max size: %d < %d", len(s.MMap), writeEnd)
		}
	}

	oldLiveBytes := s.LiveBytes
	nextOffset := s.LiveBytes + benchtop.ROW_HSIZE + compressedLen

	headerTarget := s.MMap[oldLiveBytes : oldLiveBytes+benchtop.ROW_HSIZE]
	binary.LittleEndian.PutUint32(headerTarget[:4], nextOffset)    // next row offset
	binary.LittleEndian.PutUint32(headerTarget[4:], compressedLen) // compressed size
	copy(s.MMap[oldLiveBytes+benchtop.ROW_HSIZE:], cPayload)
	s.LiveBytes = nextOffset
	// Save the buffer for next time. If the buffer allocated to be larger, use the larger one.
	s.CompressScratch = cPayload
	return &benchtop.RowLoc{
		Section: s.ID,
		Offset:  oldLiveBytes,
		Size:    compressedLen,
	}, nil
}

func (s *Section) CloseSection() error {
	if !s.Active {
		return nil
	}
	s.Lock.Lock()
	if err := s.MMap.Flush(); err != nil {
		return err
	}
	if s.File != nil {
		if err := s.File.Sync(); err != nil {
			return err
		}
	}
	s.Lock.Unlock()
	s.Active = false
	return nil
}

// RemapReadOnly converts RDWR → RDONLY mmap
func (s *Section) RemapReadOnly() error {
	if s.MMap != nil {
		s.MMap.Unmap()
	}

	roFile, err := os.Open(s.Path)
	if err != nil {
		return err
	}
	defer roFile.Close()

	m, err := mmap.Map(roFile, mmap.RDONLY, 0)
	if err != nil {
		return err
	}

	s.MMap = m
	s.MMapMode = mmap.RDONLY
	return nil
}

func (s *Section) GrowAndRemap(newSize int64) error {
	// 1. Unmap the old region
	if s.MMap != nil {
		// Crucial: ensure any pending data is flushed before unmap
		if err := s.MMap.Flush(); err != nil {
			return fmt.Errorf("flush before unmap failed: %w", err)
		}
		if err := s.File.Sync(); err != nil {
			return fmt.Errorf("sync failed: %w", err)
		}
		if err := s.MMap.Unmap(); err != nil {
			return fmt.Errorf("unmap failed: %w", err)
		}
		s.MMap = nil // Clear the old map object
	}

	// 2. Truncate the file to the new, larger size
	// Note: Use s.File (the main handle) for Truncate
	if err := s.File.Truncate(newSize); err != nil {
		return fmt.Errorf("truncate to size %d failed: %w", newSize, err)
	}

	// 3. Map the new, larger region
	newMMap, err := mmap.Map(s.File, s.MMapMode, 0)
	if err != nil {
		return fmt.Errorf("mmap failed after resize: %w", err)
	}

	s.MMap = newMMap
	return nil
}
