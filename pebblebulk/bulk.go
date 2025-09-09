package pebblebulk

import (
	"bytes"
	"io"
	"sync"

	"github.com/bmeg/benchtop/util"
	"github.com/cockroachdb/pebble"
)

const (
	maxWriterBuffer = 3 << 30
)

type PebbleBulk struct {
	Db              *pebble.DB
	Batch           *pebble.Batch
	Highest, Lowest []byte
	CurSize         int
	mu              sync.Mutex
	totalInserts    uint32
}

func (pb *PebbleBulk) Set(id []byte, val []byte, opts *pebble.WriteOptions) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.Batch == nil {
		pb.Batch = pb.Db.NewBatch()
	}

	pb.CurSize += len(id) + len(val)
	pb.totalInserts++
	if pb.Highest == nil || bytes.Compare(id, pb.Highest) > 0 {
		pb.Highest = util.CopyBytes(id)
	}
	if pb.Lowest == nil || bytes.Compare(id, pb.Lowest) < 0 {
		pb.Lowest = util.CopyBytes(id)
	}
	err := pb.Batch.Set(id, val, nil)
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

func (pb *PebbleBulk) BulkRead(fn func(tx *PebbleBulk) error) error {
	return fn(pb)
}

func (pb *PebbleBulk) Close() error {
	return pb.Db.Close()
}

func (pb *PebbleBulk) DeletePrefix(prefix []byte) error {
	nextPrefix := append(prefix, 0xFF)
	return pb.Db.DeleteRange(prefix, nextPrefix, nil)
}

func (pb *PebbleBulk) DeleteRange(start, end []byte, opts *pebble.WriteOptions) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.Batch == nil {
		pb.Batch = pb.Db.NewBatch()
	}

	if pb.Lowest == nil || bytes.Compare(start, pb.Lowest) < 0 {
		pb.Lowest = util.CopyBytes(start)
	}
	if pb.Highest == nil || bytes.Compare(end, pb.Highest) > 0 {
		pb.Highest = util.CopyBytes(end)
	}

	err := pb.Batch.DeleteRange(start, end, opts)
	if err != nil {
		return err
	}

	if pb.CurSize > maxWriterBuffer {
		if err := pb.Batch.Commit(nil); err != nil {
			return err
		}
		pb.Batch.Reset()
		pb.CurSize = 0
	}
	return nil
}
