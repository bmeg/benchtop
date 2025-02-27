package pebblebulk

import (
	"bytes"
	"io"

	"github.com/bmeg/benchtop/util"
	"github.com/cockroachdb/pebble"
)

type PebbleBulk struct {
	Db              *pebble.DB
	Batch           *pebble.Batch
	Highest, Lowest []byte
	CurSize         int
}

const (
	maxWriterBuffer = 3 << 30
)

func (pb *PebbleBulk) Set(id []byte, val []byte, opts *pebble.WriteOptions) error {
	pb.CurSize += len(id) + len(val)
	if pb.Highest == nil || bytes.Compare(id, pb.Highest) > 0 {
		pb.Highest = util.CopyBytes(id)
	}
	if pb.Lowest == nil || bytes.Compare(id, pb.Lowest) < 0 {
		pb.Lowest = util.CopyBytes(id)
	}
	err := pb.Batch.Set(id, val, opts)
	if pb.CurSize > maxWriterBuffer {
		pb.Batch.Commit(nil)
		pb.Batch.Reset()
		pb.CurSize = 0
	}
	return err
}

func (pb *PebbleBulk) Get(key []byte) ([]byte, io.Closer, error) {
	return pb.Db.Get(key)
}

func (pb *PebbleBulk) Delete(key []byte, opts *pebble.WriteOptions) error {
	return pb.Db.Delete(key, nil)
}

// Bulk functions
func (pb *PebbleBulk) BulkWrite(fn func(tx *PebbleBulk) error) error {
	if pb.Batch != nil {
		pb.Batch.Close()
	}
	pb.Batch = pb.Db.NewBatch()

	// Reset tracking fields for this batch
	pb.Lowest = nil
	pb.Highest = nil
	pb.CurSize = 0

	err := fn(pb)
	if err != nil {
		pb.Batch.Close()
		return err
	}
	err = pb.Batch.Commit(nil)
	pb.Batch.Close()
	if pb.Lowest != nil && pb.Highest != nil {
		pb.Db.Compact(pb.Lowest, pb.Highest, true)
	}
	return err
}

func (pb *PebbleBulk) BulkRead(fn func(tx *PebbleBulk) error) error {
	return fn(pb)
}

func (pb *PebbleBulk) Close() error {
	return pb.Db.Close()
}
