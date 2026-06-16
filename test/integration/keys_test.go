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

func TestFieldKeyParse_TableIDContainsFieldSepLowByte(t *testing.T) {
	tableID := uint16(31) // 0x1F in low byte
	key := benchtop.FieldKey("name", tableID, "value", []byte("row-1"))

	field, parsedTableID, value, rowID := benchtop.FieldKeyParse(key)
	if field != "name" {
		t.Fatalf("field mismatch: got %q", field)
	}
	if parsedTableID != tableID {
		t.Fatalf("table id mismatch: got %d expected %d", parsedTableID, tableID)
	}
	if value != "value" {
		t.Fatalf("value mismatch: got %#v", value)
	}
	if string(rowID) != "row-1" {
		t.Fatalf("row id mismatch: got %q", string(rowID))
	}
}

func TestFieldKeyParse_TableIDContainsFieldSepHighByte(t *testing.T) {
	tableID := uint16(7936) // 0x1F00 in little-endian high byte
	key := benchtop.FieldKey("name", tableID, "value", []byte("row-2"))

	field, parsedTableID, value, rowID := benchtop.FieldKeyParse(key)
	if field != "name" {
		t.Fatalf("field mismatch: got %q", field)
	}
	if parsedTableID != tableID {
		t.Fatalf("table id mismatch: got %d expected %d", parsedTableID, tableID)
	}
	if value != "value" {
		t.Fatalf("value mismatch: got %#v", value)
	}
	if string(rowID) != "row-2" {
		t.Fatalf("row id mismatch: got %q", string(rowID))
	}
}
