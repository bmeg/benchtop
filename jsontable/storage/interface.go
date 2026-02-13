package storage

import (
	"errors"
	"io"

	"github.com/bmeg/benchtop"
)

// RowStorage abstracts the underlying dense storage mechanism (e.g. mmap files, Parquet).
// implementations must be thread-safe.
type RowStorage interface {
	io.Closer

	// AddRow writes data to storage and returns its location.
	AddRow(data []byte, id []byte) (*benchtop.RowLoc, error)

	// AddRows writes multiple data blocks to storage and returns their locations.
	AddRows(data [][]byte, ids [][]byte) ([]*benchtop.RowLoc, error)

	// Get retrieves data from the specified location.
	Get(loc *benchtop.RowLoc) ([]byte, error)

	// GetBatch retrieves multiple data blocks from the specified location.
	GetBatch(locs []*benchtop.RowLoc) ([][]byte, []error)

	// MarkDelete marks a row as deleted (tombstone).
	MarkDelete(loc *benchtop.RowLoc) error

	// Scan iterates over storage rows, optionally filtering them.
	// Returns a channel of raw row bytes.
	Scan(concurrency int) chan []byte

	// ScanFull iterates over storage rows, returning both data and locations.
	ScanFull(concurrency int) chan benchtop.RowLocData

	// Sync ensures all written data is persisted to disk.
	Sync() error

	// Delete removes all underlying storage files.
	Delete() error

	// GetPartitionId returns the partition index for a given key.
	GetPartitionId(id []byte) int
}

// ZoneManager handles the lifecycle of storage zones (e.g. one per Project).
type ZoneManager interface {
	// GetStorage returns the storage engine for a specific Zone.
	GetStorage(zoneId string) (RowStorage, error)

	// CreateZone initializes specialized storage for a new Zone.
	CreateZone(zoneId string) (RowStorage, error)

	// DeleteZone performs a bulk-delete of an entire Zone (O(1) operation).
	DeleteZone(zoneId string) error
}

var ErrNotFound = errors.New("storage: entry not found")
