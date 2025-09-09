package pebblebulk

import (
	"io"
	"sync"

	"github.com/bmeg/grip/log"
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
	db, err := pebble.Open(path, &pebble.Options{})
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
