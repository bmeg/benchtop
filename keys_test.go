package benchtop_test

import (
	"testing"

	"github.com/bmeg/benchtop"
)

func TestIDParse(t *testing.T) {

	id := "key-0001"
	key := benchtop.NewIDKey([]byte(id))
	pID := benchtop.ParseIDKey(key)

	if id != string(pID) {
		t.Errorf("%s != %s", id, pID)
	}

}

func TestPosKeyParse(t *testing.T) {
	pos := uint64(12345)
	key := benchtop.NewPosKey(pos)
	pPos := benchtop.ParsePosKey(key)
	if pos != pPos {
		t.Errorf("%d != %d", pos, pPos)
	}
}

func TestPosValueParse(t *testing.T) {
	pos := uint64(12345)
	size := uint64(2028)

	k := benchtop.NewPosValue(pos, size)
	pPos, pSize := benchtop.ParsePosValue(k)
	if pos != pPos {
		t.Errorf("%d != %d", pos, pPos)
	}
	if size != pSize {
		t.Errorf("%d != %d", size, pSize)
	}
}
