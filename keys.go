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

// MaxIDKey stores the global counter for Mapping IDs (uint64)
var MaxIDKey = []byte{SystemMetaPrefix, 'G'}

// IDMappingPrefix (String -> Uint64)
var IDMappingPrefix = byte('I')

// RIDMappingPrefix (Uint64 -> String)
var RIDMappingPrefix = byte('B')

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
	// Expected layout:
	// F | sep | field | sep | value(json) | sep | tableID(2 bytes) | sep | rowID
	// We cannot use bytes.Split here because tableID is raw binary and may contain sep.
	if len(fieldKey) < 8 || fieldKey[0] != FieldPrefix[0] || fieldKey[1] != FieldSep[0] {
		return "", 0, nil, nil
	}

	fieldStart := 2
	fieldEndRel := bytes.IndexByte(fieldKey[fieldStart:], FieldSep[0])
	if fieldEndRel < 0 {
		return "", 0, nil, nil
	}
	fieldEnd := fieldStart + fieldEndRel

	lastSep := bytes.LastIndexByte(fieldKey, FieldSep[0])
	// Need at least 2 tableID bytes and the separator before tableID.
	if lastSep < 4 {
		return "", 0, nil, nil
	}
	tableStart := lastSep - 2
	valueEndSep := tableStart - 1
	if valueEndSep <= fieldEnd || fieldKey[valueEndSep] != FieldSep[0] {
		return "", 0, nil, nil
	}

	valueBytes := fieldKey[fieldEnd+1 : valueEndSep]
	err := sonic.ConfigFastest.Unmarshal(valueBytes, &value)
	if err != nil {
		log.Infoln("FieldKey Unmarshal Err: ", err)
	}
	tid := binary.LittleEndian.Uint16(fieldKey[tableStart:lastSep])
	rid := fieldKey[lastSep+1:]
	return string(fieldKey[fieldStart:fieldEnd]), tid, value, rid
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

// EncodeEdgeValue combines label, RowLoc, and optional inlined JSON into a single value
func EncodeEdgeValue(label string, loc *RowLoc, data map[string]any) []byte {
	lBytes := []byte(label)
	// Format: [Label]\0[Flags][Payload]
	// Flags: 0x01 (HasLoc), 0x02 (HasData)
	var flags byte
	var payload []byte
	if loc != nil {
		flags |= 0x01
		payload = append(payload, EncodeRowLoc(loc)...)
	}
	if data != nil {
		flags |= 0x02
		dBytes, _ := sonic.ConfigFastest.Marshal(data)
		payload = append(payload, dBytes...)
	}

	out := make([]byte, len(lBytes)+1+1+len(payload))
	copy(out, lBytes)
	out[len(lBytes)] = 0
	out[len(lBytes)+1] = flags
	copy(out[len(lBytes)+2:], payload)
	return out
}

// DecodeEdgeValue splits label, RowLoc, and optional inlined JSON from an integrated edge value
func DecodeEdgeValue(v []byte) (string, *RowLoc, map[string]any) {
	idx := bytes.IndexByte(v, 0)
	if idx < 0 {
		return "", DecodeRowLoc(v), nil
	}
	label := string(v[:idx])
	if len(v) <= idx+1 {
		return label, nil, nil
	}
	flags := v[idx+1]
	payload := v[idx+2:]

	var loc *RowLoc
	var data map[string]any
	offset := 0
	if flags&0x01 != 0 {
		if len(payload) >= 14 {
			loc = DecodeRowLoc(payload[:14])
			offset = 14
		}
	}
	if flags&0x02 != 0 {
		if len(payload) > offset {
			sonic.ConfigFastest.Unmarshal(payload[offset:], &data)
		}
	}
	return label, loc, data
}
