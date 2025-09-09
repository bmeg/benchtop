package section

import (
	"fmt"
	"os"
	"sync"
)

const (
	SECTION_FILE_SUFFIX string = ".partition"
	SECTION_ID_MULT     uint16 = 16384
	HEADER_SIZE         uint32 = 8
)

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

func (s *Section) WriteJsonEntryToSection(payload []byte) (uint32, error) {
	_, err := s.Handle.Write(payload)
	if err != nil {
		return 0, fmt.Errorf("failed to write payload: %w", err)
	}
	return uint32(len(payload)) - HEADER_SIZE, nil
}
