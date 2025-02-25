package pebblebulk

import (
	"bytes"
	"io"

	"github.com/bmeg/benchtop/util"
	"github.com/cockroachdb/pebble"
)

type PebbleBulkWrite struct {
	Db              *pebble.DB
	Batch           *pebble.Batch
	Highest, Lowest []byte
	CurSize         int
}

type PebbleBulkRead struct {
	Db *pebble.DB
}

const (
	maxWriterBuffer = 3 << 30
)

func (pbw *PebbleBulkWrite) Set(id []byte, val []byte, opts *pebble.WriteOptions) error {
	pbw.CurSize += len(id) + len(val)
	if pbw.Highest == nil || bytes.Compare(id, pbw.Highest) > 0 {
		pbw.Highest = util.CopyBytes(id)
	}
	if pbw.Lowest == nil || bytes.Compare(id, pbw.Lowest) < 0 {
		pbw.Lowest = util.CopyBytes(id)
	}
	err := pbw.Batch.Set(id, val, opts)
	if pbw.CurSize > maxWriterBuffer {
		pbw.Batch.Commit(nil)
		pbw.Batch.Reset()
		pbw.CurSize = 0
	}
	return err
}

func (pbr *PebbleBulkRead) Get(key []byte) ([]byte, io.Closer, error) {
	return pbr.Db.Get(key)
}

func (pbw *PebbleBulkWrite) Delete(key []byte, opts *pebble.WriteOptions) error {
	return pbw.Db.Delete(key, nil)
}
