package benchtop

import (
	"encoding/binary"
)

var IdPrefix = byte('T')
var NamePrefix = byte('t')
var posPrefix = byte('p')

/* Name keys used for storing the key names of rows in a table*/
func NewNameKey(id []byte) []byte {
	out := make([]byte, len(id)+1)
	out[0] = NamePrefix
	for i := 0; i < len(id); i++ {
		out[i+1] = id[i]
	}
	return out
}

func ParseNameKey(key []byte) []byte {
	//duplicate the key, because pebble reuses memory
	out := make([]byte, len(key)-1)
	for i := 0; i < len(key)-1; i++ {
		out[i] = key[i+1]
	}
	return out
}

/* Id Keys used for storing table id */
func NewIDKey(id uint32) []byte {
	out := make([]byte, 5)
	out[0] = IdPrefix
	binary.LittleEndian.PutUint32(out[1:], id)
	return out
}

func ParseIDKey(key []byte) uint32 {
	return binary.LittleEndian.Uint32(key[1:])
}

func NewPosKey(table uint32, name []byte) []byte {
	out := make([]byte, 5+len(name))
	out[0] = posPrefix
	binary.LittleEndian.PutUint32(out[1:], table)
	for i := 0; i < len(name); i++ {
		out[i+5] = name[i]
	}
	return out
}

func ParsePosKey(key []byte) (uint32, []byte) {
	//duplicate the key, because pebble reuses memory
	out := make([]byte, len(key)-5)
	for i := 0; i < len(key)-5; i++ {
		out[i] = key[i+5]
	}
	return binary.LittleEndian.Uint32(key[1:]), out
}

func NewPosKeyPrefix(table uint32) []byte {
	out := make([]byte, 5)
	out[0] = posPrefix
	binary.LittleEndian.PutUint32(out[1:], table)
	return out
}

func NewPosValue(offset uint64, size uint64) []byte {
	out := make([]byte, 64)
	binary.LittleEndian.PutUint64(out, offset)
	binary.LittleEndian.PutUint64(out[8:], size)
	return out
}

func ParsePosValue(v []byte) (uint64, uint64) {
	return binary.LittleEndian.Uint64(v), binary.LittleEndian.Uint64(v[8:])
}
