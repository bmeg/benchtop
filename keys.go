package benchtop

import "encoding/binary"

var keyPrefix = byte('k')
var posPrefix = byte('p')

func NewIDKey(id []byte) []byte {
	out := make([]byte, len(id)+1)
	out[0] = keyPrefix
	for i := 0; i < len(id); i++ {
		out[i+1] = id[i]
	}
	return out
}

func ParseIDKey(key []byte) []byte {
	return key[1:]
}

func NewPosKey(pos uint64) []byte {
	out := make([]byte, 9)
	out[0] = posPrefix
	binary.LittleEndian.PutUint64(out[1:], pos)
	return out
}

func ParsePosKey(key []byte) uint64 {
	return binary.LittleEndian.Uint64(key[1:])
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
