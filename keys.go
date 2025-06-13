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

// RowTableAsociation Reverse index
// Key: R
// given an ID return the table uint32 associated with it
var RowTableAsocPrefix = byte('R')

// Position
// key: P | TableId | Position
// The position and offset of the document.
var PosPrefix = byte('P')

// Field
// key: F
// used for indexing specific field values in kvgraph
var FieldPrefix = []byte("F")

// The '0x1F' invisible character unit seperator not supposed to appear in ASCII text
var FieldSep = []byte{0x1F}

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
			valueBytes,    // BSON-encoded value
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

func NewRowTableAsocKey(id []byte) []byte {
	out := make([]byte, len(id)+1)
	out[0] = RowTableAsocPrefix
	copy(out[1:], id)
	return out
}

func ParseTableAsocKey(key []byte) []byte {
	//duplicate the key, because pebble reuses memory
	out := make([]byte, len(key)-1)
	copy(out, key[1:])
	return out
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
func NewPosKey(table uint32, name []byte) []byte {
	out := make([]byte, 5+len(name))
	out[0] = PosPrefix
	binary.LittleEndian.PutUint32(out[1:], table)
	copy(out[5:], name)
	return out
}

func ParsePosKey(key []byte) (uint32, []byte) {
	//duplicate the key, because pebble reuses memory
	out := make([]byte, len(key)-5)
	copy(out, key[5:])
	return binary.LittleEndian.Uint32(key[1:5]), out
}

func NewPosKeyPrefix(table uint32) []byte {
	var out [5]byte
	out[0] = PosPrefix
	binary.LittleEndian.PutUint32(out[1:], table)
	return out[:]
}

func NewPosValue(offset uint64, size uint64) []byte {
	var out [64]byte
	binary.LittleEndian.PutUint64(out[:], offset)
	binary.LittleEndian.PutUint64(out[8:], size)
	return out[:]
}

func ParsePosValue(v []byte) (uint64, uint64) {
	return binary.LittleEndian.Uint64(v), binary.LittleEndian.Uint64(v[8:])
}
