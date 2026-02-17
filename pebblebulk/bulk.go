package pebblebulk

import (
	"bytes"
	"io"
	"sync"

	"github.com/bmeg/benchtop/util"
	"github.com/cockroachdb/pebble"
)

const (
	maxWriterBuffer = 16 << 20
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

	if err := pb.Batch.Set(id, val, nil); err != nil {
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

func (pb *PebbleBulk) Get(key []byte) ([]byte, io.Closer, error) {
	return pb.Db.Get(key)
}

func (pb *PebbleBulk) Delete(key []byte, opts *pebble.WriteOptions) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.Batch == nil {
		pb.Batch = pb.Db.NewBatch()
	}

	if err := pb.Batch.Delete(key, nil); err != nil {
		return err
	}

	pb.CurSize += len(key)
	if pb.CurSize > maxWriterBuffer {
		if err := pb.Batch.Commit(nil); err != nil {
			return err
		}
		pb.Batch.Reset()
		pb.CurSize = 0
	}
	return nil
}

func (pb *PebbleBulk) BulkRead(fn func(tx *PebbleBulk) error) error {
	return fn(pb)
}

func (pb *PebbleBulk) Close() error {
	if pb.Batch != nil {
		pb.Batch.Commit(nil)
		pb.Batch.Close()
	}
	return pb.Db.Close()
}

func (pb *PebbleBulk) DeletePrefix(prefix []byte) error {
	// Standard way to get range end for prefix deletion in Pebble/LevelDB
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix[:i+1])
			limit[i]++
			break
		}
	}

	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.Batch == nil {
		pb.Batch = pb.Db.NewBatch()
	}

	// DeleteRange is [start, end) exclusive. limit is the first key that doesn't start with prefix.
	if err := pb.Batch.DeleteRange(prefix, limit, nil); err != nil {
		return err
	}

	pb.CurSize += len(prefix) + len(limit)
	if pb.CurSize > maxWriterBuffer {
		if err := pb.Batch.Commit(nil); err != nil {
			return err
		}
		pb.Batch.Reset()
		pb.CurSize = 0
	}
	return nil
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

	pb.CurSize += len(start) + len(end)
	if pb.CurSize > maxWriterBuffer {
		if err := pb.Batch.Commit(nil); err != nil {
			return err
		}
		pb.Batch.Reset()
		pb.CurSize = 0
	}
	return nil
}
