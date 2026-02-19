package benchtop

import (
	"bytes"
	"encoding/binary"

	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
)

const (
	ROW_HSIZE        uint32 = 8 // Header size: 4-byte next offset + 4-byte size
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

// builds a RFieldKey in the format "R | TableID | field | rowId"
func RFieldKey(tableID uint16, field, rowID string) []byte {
	idBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(idBytes, tableID)
	return bytes.Join([][]byte{
		RFieldPrefix,
		idBytes,
		[]byte(field),
		[]byte(rowID),
	}, FieldSep)
}

// System Metadata
// key: S
var SystemMetaPrefix = byte('S')

// MaxTableIDKey stores the global counter for Table IDs
var MaxTableIDKey = []byte{SystemMetaPrefix, 'I'}

func FieldKey(field string, tableID uint16, value any, rowID []byte) []byte {
	/* creates a full field key for optimizing the beginning of a query */
	valueBytes, err := sonic.ConfigFastest.Marshal(value)
	if err != nil {
		log.Infoln("FieldKey Marshal Err: ", err)
	}
	idBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(idBytes, tableID)
	// NEW ORDER: F | field | value | tableID | rowID
	return bytes.Join(
		[][]byte{
			FieldPrefix,   // Static prefix
			[]byte(field), // field name
			valueBytes,    // JSON-encoded value
			idBytes,       // table ID
			rowID,
		},
		FieldSep,
	)
}

func FieldKeyParse(fieldKey []byte) (field string, tableID uint16, value any, rowID []byte) {
	parts := bytes.Split(fieldKey, FieldSep)
	if len(parts) < 5 {
		return "", 0, nil, nil
	}
	// With the new order, value is parts[2], tableID is parts[3], rowID is parts[len-1]
	err := sonic.ConfigFastest.Unmarshal(parts[2], &value)
	if err != nil {
		log.Infoln("FieldKey Unmarshal Err: ", err)
	}
	tid := binary.LittleEndian.Uint16(parts[3])
	rid := parts[len(parts)-1]
	return string(parts[1]), tid, value, rid
}

// FieldValueKey returns a prefix for global seek of a specific field value across all tables
func FieldValueKey(field string, value any) []byte {
	valueBytes, err := sonic.ConfigFastest.Marshal(value)
	if err != nil {
		log.Infoln("FieldValueKey Marshal Err: ", err)
		return nil
	}
	return bytes.Join(
		[][]byte{
			FieldPrefix,
			[]byte(field),
			valueBytes,
		},
		FieldSep,
	)
}

func FieldLabelKey(field string, tableID uint16) []byte {
	idBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(idBytes, tableID)
	// NOTE: This can no longer be used as a simple Prefix for DeletePrefix if value is in the middle.
	// But it is still used for individual key construction in some legacy paths.
	return bytes.Join(
		[][]byte{
			FieldPrefix,   // Static prefix
			[]byte(field), // table field
			idBytes,       // table ID (Legacy order compatibility where needed, though primary uses FieldKey)
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
	var out [14]byte
	binary.LittleEndian.PutUint16(out[0:], loc.TableId)
	binary.LittleEndian.PutUint16(out[2:], loc.Section)
	binary.LittleEndian.PutUint32(out[4:], loc.Offset)
	binary.LittleEndian.PutUint32(out[8:], loc.Size)
	binary.LittleEndian.PutUint16(out[12:], loc.Index)
	return out[:]
}

func DecodeRowLoc(v []byte) *RowLoc {
	if len(v) < 12 {
		return nil
	}

	loc := &RowLoc{
		TableId: binary.LittleEndian.Uint16(v[0:]),
		Section: binary.LittleEndian.Uint16(v[2:]),
		Offset:  binary.LittleEndian.Uint32(v[4:]),
		Size:    binary.LittleEndian.Uint32(v[8:]),
	}

	// Data that is all zeros is considered invalid (not found/legacy)
	// Especially if TableId and Section are both 0, it's likely uninitialized.
	if loc.TableId == 0 && loc.Section == 0 && loc.Offset == 0 && loc.Size == 0 {
		return nil
	}

	if len(v) >= 14 {
		loc.Index = binary.LittleEndian.Uint16(v[12:])
	}
	return loc
}

// Integrated Helpers for Grids

// EncodeVertexValue combines label and RowLoc into a single value
func EncodeVertexValue(label string, loc *RowLoc) []byte {
	lBytes := []byte(label)
	out := make([]byte, len(lBytes)+1+14)
	copy(out, lBytes)
	out[len(lBytes)] = 0
	if loc != nil {
		copy(out[len(lBytes)+1:], EncodeRowLoc(loc))
	}
	return out
}

// DecodeVertexValue splits label and RowLoc from an integrated value
func DecodeVertexValue(v []byte) (string, *RowLoc) {
	idx := bytes.IndexByte(v, 0)
	if idx < 0 {
		return string(v), nil
	}
	label := string(v[:idx])
	locBytes := v[idx+1:]
	if len(locBytes) >= 12 {
		return label, DecodeRowLoc(locBytes)
	}
	return label, nil
}

// EncodeEdgeValue combines label and RowLoc into a single value
func EncodeEdgeValue(label string, loc *RowLoc) []byte {
	lBytes := []byte(label)
	out := make([]byte, len(lBytes)+1+14)
	copy(out, lBytes)
	out[len(lBytes)] = 0
	if loc != nil {
		copy(out[len(lBytes)+1:], EncodeRowLoc(loc))
	}
	return out
}

// DecodeEdgeValue splits label and RowLoc from an integrated edge value
func DecodeEdgeValue(v []byte) (string, *RowLoc) {
	idx := bytes.IndexByte(v, 0)
	if idx < 0 {
		return "", DecodeRowLoc(v)
	}
	label := string(v[:idx])
	locBytes := v[idx+1:]
	if len(locBytes) >= 12 {
		return label, DecodeRowLoc(locBytes)
	}
	return label, nil
}
