package pebblebulk

import (
	"io"

	"github.com/cockroachdb/pebble"
)

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
