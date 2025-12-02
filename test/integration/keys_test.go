package test

import (
	"testing"

	"github.com/bmeg/benchtop"
)

func TestIDParse(t *testing.T) {
	id := "key-0001"
	key := benchtop.NewTableKey([]byte(id))
	pID := benchtop.ParseTableKey(key)

	if id != string(pID) {
		t.Errorf("%s != %s", string(id), string(pID))
	}
}

func TestPosKeyParse(t *testing.T) {
	tableId := uint16(5)
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
	pos := uint32(12345)
	size := uint32(2028)
	tableId := uint16(0)
	section := uint16(0)

	k := benchtop.EncodeRowLoc(&benchtop.RowLoc{TableId: tableId, Section: section, Offset: pos, Size: size})
	loc := benchtop.DecodeRowLoc(k)
	if pos != loc.Offset {
		t.Errorf("%d != %d", pos, loc.Offset)
	}
	if size != loc.Size {
		t.Errorf("%d != %d", size, loc.Size)
	}
	if section != loc.Section {
		t.Errorf("%d != %d", size, loc.Size)
	}
	if tableId != loc.TableId {
		t.Errorf("%d != %d", size, loc.Size)
	}
}
