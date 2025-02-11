package test

import (
	"testing"

	"github.com/bmeg/benchtop"
)

func TestIDParse(t *testing.T) {

	id := "key-0001"
	key := benchtop.NewNameKey([]byte(id))
	pID := benchtop.ParseNameKey(key)

	if id != string(pID) {
		t.Errorf("%s != %s", id, pID)
	}

}

func TestPosKeyParse(t *testing.T) {
	tableId := uint32(5)
	name := []byte("MyKey")

	key := benchtop.NewPosKey(tableId, name)
	nTableId, nName := benchtop.ParsePosKey(key)
	if tableId != nTableId {
		t.Errorf("%d != %d", tableId, nTableId)
	}
	if string(name) != string(nName) {
		t.Errorf("%d != %d", name, nName)
		t.Errorf("%s != %s", string(name), string(nName))
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
