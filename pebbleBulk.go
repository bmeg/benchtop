package benchtop

import (
	"bytes"
	"io"

	"github.com/bmeg/benchtop/util"
	"github.com/cockroachdb/pebble"
)

type pebbleBulkWrite struct {
	db              *pebble.DB
	batch           *pebble.Batch
	highest, lowest []byte
	curSize         int
}

type pebbleBulkRead struct {
	db *pebble.DB
}

const (
	maxWriterBuffer = 3 << 30
)

func (pbw *pebbleBulkWrite) Set(id []byte, val []byte, opts *pebble.WriteOptions) error {
	pbw.curSize += len(id) + len(val)
	if pbw.highest == nil || bytes.Compare(id, pbw.highest) > 0 {
		pbw.highest = util.CopyBytes(id)
	}
	if pbw.lowest == nil || bytes.Compare(id, pbw.lowest) < 0 {
		pbw.lowest = util.CopyBytes(id)
	}
	err := pbw.batch.Set(id, val, opts)
	if pbw.curSize > maxWriterBuffer {
		pbw.batch.Commit(nil)
		pbw.batch.Reset()
		pbw.curSize = 0
	}
	return err
}

func (pbr *pebbleBulkRead) Get(key []byte) ([]byte, io.Closer, error) {
	return pbr.db.Get(key)
}

func (pbw *pebbleBulkWrite) Delete(key []byte, opts *pebble.WriteOptions) error {
	return pbw.db.Delete(key, nil)
}

func (b *BSONTable) bulkGet(u func(s dbGet) error) error {
	return u(&pebbleBulkRead{b.db})
}

func (b *BSONTable) bulkSet(u func(s dbSet) error) error {
	batch := b.db.NewBatch()
	ptx := &pebbleBulkWrite{b.db, batch, nil, nil, 0}
	err := u(ptx)
	batch.Commit(nil)
	batch.Close()
	if ptx.lowest != nil && ptx.highest != nil {
		b.db.Compact(ptx.lowest, ptx.highest, true)
	}
	return err
}

func (b *BSONTable) bulkDelete(u func(s dbDelete) error) error {
	batch := b.db.NewBatch()
	ptx := &pebbleBulkWrite{b.db, batch, nil, nil, 0}
	err := u(ptx)
	batch.Commit(nil)
	batch.Close()
	if ptx.lowest != nil && ptx.highest != nil {
		b.db.Compact(ptx.lowest, ptx.highest, true)
	}
	return err
}
