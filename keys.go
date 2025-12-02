package benchtop

import (
	"bytes"
	"encoding/binary"
	"encoding/json"

	"github.com/bmeg/grip/log"
)

const (
	ROW_HSIZE        uint32 = 8 // Header size: 8-byte next offset + 4-byte size
	ROW_OFFSET_HSIZE uint32 = 4 // Offset part of header
)

// Vertex TableId
// key: T | TableId | VtablePrefix'
// The starting point for vertex table ids in th pebble index
var TablePrefix = byte('T')

// Position
// key: P | TableId | Position
// The position and offset of the document.
var PosPrefix = byte('P')

// Field
// key: F
// used for indexing specific field values
var FieldPrefix = []byte{'F'}

// ReverseField Index
// key: R
// used for reverse indexing specific field keys in order to be able to efficiently delete indices
var RFieldPrefix = []byte{'R'}

// The '0x1F' invisible character unit seperator not supposed to appear in ASCII text
var FieldSep = []byte{0x1F}

// builds a RFieldKey in the format "R 0x1F label 0x1F field 0x1F rowId"
func RFieldKey(label, field, rowID string) []byte {
	return bytes.Join([][]byte{
		RFieldPrefix,
		[]byte(label),
		[]byte(field),
		[]byte(rowID),
	}, FieldSep)
}

func FieldKey(field string, label string, value any, rowID []byte) []byte {
	/* creates a full field key for optimizing the beginning of a query */
	valueBytes, err := json.Marshal(value)
	if err != nil {
		log.Infoln("FieldKey Marshal Err: ", err)
	}
	return bytes.Join(
		[][]byte{
			FieldPrefix,   // Static prefix
			[]byte(field), // table field
			[]byte(label), // label
			valueBytes,    // JSON-encoded value
			rowID,
		},
		FieldSep,
	)
}

func FieldKeyParse(fieldKey []byte) (field, label string, value any, rowID []byte) {
	parts := bytes.Split(fieldKey, FieldSep)
	err := json.Unmarshal(parts[3], &value)
	if err != nil {
		log.Infoln("FieldKey Unmarshal Err: ", err)
	}
	return string(parts[1]), string(parts[2]), value, parts[4]
}

func FieldLabelKey(field, label string) []byte {
	return bytes.Join(
		[][]byte{
			FieldPrefix,   // Static prefix
			[]byte(field), // table field
			[]byte(label), // label
		},
		FieldSep,
	)
}

func NewTableKey(id []byte) []byte {
	out := make([]byte, len(id)+1)
	out[0] = TablePrefix
	copy(out[1:], id)
	return out
}

func ParseTableKey(key []byte) []byte {
	//duplicate the key, because pebble reuses memory
	out := make([]byte, len(key)-1)
	copy(out, key[1:])
	return out
}

/* New pos key used for creating a pos key from a table entry*/
func NewPosKey(table uint16, name []byte) []byte {
	out := make([]byte, 3+len(name))
	out[0] = PosPrefix
	binary.LittleEndian.PutUint16(out[1:], table)
	copy(out[3:], name)
	return out
}

func ParsePosKey(key []byte) (uint16, []byte) {
	//duplicate the key, because pebble reuses memory
	out := make([]byte, len(key)-3)
	copy(out, key[3:])
	return binary.LittleEndian.Uint16(key[1:3]), out
}

func NewPosKeyPrefix(table uint16) []byte {
	var out [3]byte
	out[0] = PosPrefix
	binary.LittleEndian.PutUint16(out[1:], table)
	return out[:]
}

/*
Builds a 12 byte row loc encoding

	Each encoding in order contains:

	2 bytes for TableId
	2 bytes for SectionId
	4 bytes for Offset
	4 bytes for Size
*/
func EncodeRowLoc(loc *RowLoc) []byte {
	var out [12]byte
	binary.LittleEndian.PutUint16(out[0:], loc.TableId)
	binary.LittleEndian.PutUint16(out[2:], loc.Section)
	binary.LittleEndian.PutUint32(out[4:], loc.Offset)
	binary.LittleEndian.PutUint32(out[8:], loc.Size)
	return out[:]
}

func DecodeRowLoc(v []byte) *RowLoc {
	return &RowLoc{
		TableId: binary.LittleEndian.Uint16(v[0:]),
		Section: binary.LittleEndian.Uint16(v[2:]),
		Offset:  binary.LittleEndian.Uint32(v[4:]),
		Size:    binary.LittleEndian.Uint32(v[8:]),
	}
}
