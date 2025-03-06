package pebblebulk

import (
	"bytes"
	"io"
	"sync"

	"github.com/bmeg/benchtop/util"
	"github.com/bmeg/grip/log"
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

type PebbleKV struct {
	Db           *pebble.DB
	InsertCount  uint32
	CompactLimit uint32
}

func (pb *PebbleBulk) Set(id []byte, val []byte, opts *pebble.WriteOptions) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

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

func (pdb *PebbleKV) Set(id []byte, val []byte) error {
	return pdb.Db.Set(id, val, nil)
}

func (pdb *PebbleKV) BulkWrite(u func(tx *PebbleBulk) error) error {
	batch := pdb.Db.NewBatch()
	ptx := &PebbleBulk{pdb.Db, batch, nil, nil, 0, sync.Mutex{}, 0}
	err := u(ptx)
	batch.Commit(nil)
	batch.Close()

	pdb.InsertCount += ptx.totalInserts
	if pdb.InsertCount > pdb.CompactLimit {
		log.Debugf("Running pebble compact %d > %d", pdb.InsertCount, pdb.CompactLimit)
		pdb.Db.Compact([]byte{0x00}, []byte{0xFF}, true)
		pdb.InsertCount = 0
	}
	return err
}

func (pb *PebbleKV) View(u func(tx *PebbleIterator) error) error {
	it, err := pb.Db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return err
	}
	pit := &PebbleIterator{pb.Db, it, true, nil, nil}
	err = u(pit)
	it.Close()
	return err
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

type PebbleIterator struct {
	db      *pebble.DB
	iter    *pebble.Iterator
	forward bool
	key     []byte
	value   []byte
}

func (pit *PebbleIterator) Key() []byte {
	return pit.key
}

func (pit *PebbleIterator) Valid() bool {
	return pit.iter.Valid()
}

func (pit *PebbleIterator) Value() ([]byte, error) {
	return pit.value, nil
}

func (pit *PebbleIterator) Get(id []byte) ([]byte, error) {
	v, c, err := pit.db.Get(id)
	if err != nil {
		return nil, err
	}
	out := copyBytes(v)
	c.Close()
	return out, nil
}

func (pit *PebbleIterator) Seek(id []byte) error {
	pit.forward = true
	if !pit.iter.SeekGE(id) {
		return io.EOF
	}
	pit.key = copyBytes(pit.iter.Key())
	pit.value = copyBytes(pit.iter.Value())
	return nil
}

func (pit *PebbleIterator) Next() error {
	if pit.forward {
		if !pit.iter.Next() {
			return io.EOF
		}
	} else {
		if !pit.iter.Prev() {
			return io.EOF
		}
	}
	pit.key = copyBytes(pit.iter.Key())
	pit.value = copyBytes(pit.iter.Value())
	return nil
}

func copyBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
