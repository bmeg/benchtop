package benchtop

import (
	"bytes"
	"encoding/binary"
	"encoding/json"

	"github.com/bmeg/grip/log"
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

func NewPosValue(offset uint64, size uint64) []byte {
	var out [64]byte
	binary.LittleEndian.PutUint64(out[:], offset)
	binary.LittleEndian.PutUint64(out[8:], size)
	return out[:]
}

func ParsePosValue(v []byte) (offset uint64, size uint64) {
	return binary.LittleEndian.Uint64(v), binary.LittleEndian.Uint64(v[8:])
}
