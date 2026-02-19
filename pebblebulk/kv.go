package pebblebulk

import (
	"io"
	"runtime"
	"sync"

	"github.com/cockroachdb/pebble"
)

type KVStore interface {
	Get(key []byte) ([]byte, io.Closer, error)
	View(func(it *PebbleIterator) error) error
	Set(key, value []byte, opts *pebble.WriteOptions) error
	Delete(key []byte, opts *pebble.WriteOptions) error
	BulkWrite(func(tx *PebbleBulk) error) error
	Close() error
}

type PebbleKV struct {
	Db           *pebble.DB
	InsertCount  uint32
	CompactLimit uint32
	mu           sync.Mutex
}

func NewPebbleKV(path string) (*PebbleKV, error) {
	// 512 MB cache
	cache := pebble.NewCache(512 << 20)
	opts := &pebble.Options{
		Cache:        cache,
		MemTableSize: 256 << 20,
		// Keep ingest from hitting aggressive write stalls under bulk load.
		L0CompactionThreshold: 8,
		L0StopWritesThreshold: 128,
		LBaseMaxBytes:         512 << 20,
		MaxConcurrentCompactions: func() int {
			n := runtime.GOMAXPROCS(0) / 2
			if n < 1 {
				return 1
			}
			if n > 8 {
				return 8
			}
			return n
		},
	}
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return &PebbleKV{
		Db:           db,
		InsertCount:  0,
		CompactLimit: uint32(1000),
		mu:           sync.Mutex{},
	}, nil
}

func (pdb *PebbleKV) Set(id []byte, val []byte, opts *pebble.WriteOptions) error {
	return pdb.Db.Set(id, val, opts)
}

func (pdb *PebbleKV) BulkWrite(u func(tx *PebbleBulk) error) error {
	batch := pdb.Db.NewBatch()
	ptx := &PebbleBulk{pdb.Db, batch, nil, nil, 0, sync.Mutex{}, 0}
	err := u(ptx)
	if err != nil {
		batch.Close()
		return err
	}
	// Only commit if there is uncommitted data remaining in the batch.
	// PebbleBulk.Set() does intermediate commit+reset when CurSize exceeds
	// the threshold, so the batch may already be empty.
	if ptx.CurSize > 0 {
		// log.Printf("[BulkWrite] final batch.Commit curSize=%d totalInserts=%d", ptx.CurSize, ptx.totalInserts)
		if err := batch.Commit(nil); err != nil {
			batch.Close()
			return err
		}
		// log.Printf("[BulkWrite] final batch.Commit DONE")
	} else {
		// log.Printf("[BulkWrite] skipping final commit, batch already flushed (totalInserts=%d)", ptx.totalInserts)
	}
	batch.Close()

	return nil
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

func (pb *PebbleKV) Close() error {
	return pb.Db.Close()
}

func (pb *PebbleKV) Delete(key []byte, opts *pebble.WriteOptions) error {
	return pb.Db.Delete(key, opts)
}

func (pb *PebbleKV) Get(key []byte) ([]byte, io.Closer, error) {
	val, closer, err := pb.Db.Get(key)
	return val, closer, err
}
