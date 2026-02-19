package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/DataDog/zstd"
	"github.com/pierrec/lz4/v4"
)

// Block represents a collection of rows that are compressed together.
type Block struct {
	Rows [][]byte
}

func NewBlock(capacity int) *Block {
	return &Block{
		Rows: make([][]byte, 0, capacity),
	}
}

func (b *Block) Add(row []byte) {
	b.Rows = append(b.Rows, row)
}

func (b *Block) Count() int {
	return len(b.Rows)
}

const (
	ZSTD_MAGIC = 0xFD2FB528
)

// Serialize packs the block into bytes and compresses it.
// Format: [Count uint16][Offsets uint32...][Data...]
func (b *Block) Serialize(pool *sync.Pool) ([]byte, error) {
	count := len(b.Rows)
	if count == 0 {
		return nil, nil
	}

	headerSize := 2 + 4*count
	totalDataSize := 0
	for _, r := range b.Rows {
		totalDataSize += len(r)
	}
	totalSize := headerSize + totalDataSize

	rawBuf := make([]byte, totalSize)

	// Write Count
	binary.LittleEndian.PutUint16(rawBuf[0:], uint16(count))

	// Write Offsets and Data
	currentOffset := uint32(0)
	dataStart := rawBuf[headerSize:]
	offsetPtr := 2

	for _, r := range b.Rows {
		// Write Offset
		binary.LittleEndian.PutUint32(rawBuf[offsetPtr:], currentOffset)
		offsetPtr += 4

		// Write Data
		copy(dataStart[currentOffset:], r)
		currentOffset += uint32(len(r))
	}

	// Compress
	cBufObj := pool.Get()
	var cBuf []byte
	if cBufObj != nil {
		cBuf = cBufObj.([]byte)
	}

	// zstd.Compress might return a new slice if cBuf is too small
	compressed, err := zstd.Compress(cBuf[:0], rawBuf)
	if err != nil {
		if cBuf != nil {
			pool.Put(cBuf[:0])
		}
		return nil, fmt.Errorf("compression failed: %w", err)
	}

	// Verification: Zstd magic number should be present
	if len(compressed) >= 4 {
		magic := binary.LittleEndian.Uint32(compressed[0:4])
		if magic != ZSTD_MAGIC {
			if cBuf != nil {
				pool.Put(cBuf[:0])
			}
			return nil, fmt.Errorf("zstd compression produced invalid magic: %08x (expected %08x)", magic, ZSTD_MAGIC)
		}
	} else if len(compressed) > 0 {
		if cBuf != nil {
			pool.Put(cBuf[:0])
		}
		return nil, fmt.Errorf("zstd compression produced too short output: %d bytes", len(compressed))
	}

	// Make a copy since buffer is reused or might be part of pool buffer
	out := make([]byte, len(compressed))
	copy(out, compressed)

	if cBuf != nil {
		pool.Put(cBuf[:0])
	}

	return out, nil
}

// ExtractRow decompresses the block and returns the specific row at index.
func ExtractRow(compressed []byte, index uint16, pool *sync.Pool) ([]byte, error) {
	outBuf := pool.Get().([]byte)
	defer pool.Put(outBuf[:0])

	decompressed, err := decompressCompat(outBuf[:0], compressed)
	if err != nil {
		return nil, fmt.Errorf("decompress failed: %w", err)
	}

	if len(decompressed) < 2 {
		return nil, fmt.Errorf("invalid block: too short")
	}

	count := binary.LittleEndian.Uint16(decompressed[0:])
	if int(index) >= int(count) {
		return nil, fmt.Errorf("index %d out of bounds (count %d)", index, count)
	}

	offsetIdx := 2 + int(index)*4
	if offsetIdx+4 > len(decompressed) {
		return nil, fmt.Errorf("corrupt block header")
	}
	offset := binary.LittleEndian.Uint32(decompressed[offsetIdx:])

	var nextOffset uint32
	if int(index)+1 < int(count) {
		nextOffset = binary.LittleEndian.Uint32(decompressed[offsetIdx+4:])
	} else {
		nextOffset = uint32(len(decompressed) - (2 + 4*int(count)))
	}

	dataStart := 2 + 4*int(count)
	start := dataStart + int(offset)
	end := dataStart + int(nextOffset)

	if start > len(decompressed) || end > len(decompressed) {
		return nil, fmt.Errorf("block data out of bounds")
	}

	// Copy result to return safe byte slice
	result := make([]byte, end-start)
	copy(result, decompressed[start:end])
	return result, nil
}

// IterateBlock decompresses the block and calls the callback for each row.
// Returns error if decompression fails.
func IterateBlock(compressed []byte, pool *sync.Pool, callback func([]byte) bool) error {
	outBuf := pool.Get().([]byte)
	defer pool.Put(outBuf[:0])

	decompressed, err := decompressCompat(outBuf[:0], compressed)
	if err != nil {
		return fmt.Errorf("decompress failed: %w", err)
	}

	if len(decompressed) < 2 {
		return fmt.Errorf("invalid block: too short")
	}

	count := binary.LittleEndian.Uint16(decompressed[0:])
	dataStart := 2 + 4*int(count)

	for i := 0; i < int(count); i++ {
		offsetIdx := 2 + i*4
		if offsetIdx+4 > len(decompressed) {
			return fmt.Errorf("corrupt block header")
		}
		offset := binary.LittleEndian.Uint32(decompressed[offsetIdx:])

		var nextOffset uint32
		if i+1 < int(count) {
			nextOffset = binary.LittleEndian.Uint32(decompressed[offsetIdx+4:])
		} else {
			nextOffset = uint32(len(decompressed) - dataStart)
		}

		start := dataStart + int(offset)
		end := dataStart + int(nextOffset)

		if start > len(decompressed) || end > len(decompressed) {
			return fmt.Errorf("block key out of bouds")
		}

		if !callback(decompressed[start:end]) {
			return nil
		}
	}
	return nil
}

// DecompressBlock decompresses the block and returns the raw bytes.
// It allocates a new slice for the result which is suitable for long-term caching.
func DecompressBlock(compressed []byte) ([]byte, error) {
	// We do not use the pool here because we want the result to persist in the cache.
	decompressed, err := decompressCompat(nil, compressed)
	if err != nil {
		hexStr := ""
		for i := 0; i < len(compressed) && i < 32; i++ {
			hexStr += fmt.Sprintf("%02x ", compressed[i])
		}
		return nil, fmt.Errorf("decompress failed (len=%d header=[%s]): %w", len(compressed), hexStr, err)
	}
	return decompressed, nil
}

func decompressCompat(dst []byte, payload []byte) ([]byte, error) {
	// Current format: zstd compressed block
	if out, err := zstd.Decompress(dst, payload); err == nil {
		return out, nil
	}

	// Legacy format support: lz4 frame compressed block
	lz4r := lz4.NewReader(bytes.NewReader(payload))
	if out, err := io.ReadAll(lz4r); err == nil {
		return out, nil
	}

	// Already-uncompressed block
	if isValidBlockLayout(payload) {
		out := make([]byte, len(payload))
		copy(out, payload)
		return out, nil
	}

	// Legacy single-row payload (raw JSON row bytes)
	if len(payload) > 0 {
		i := 0
		for i < len(payload) && (payload[i] == ' ' || payload[i] == '\n' || payload[i] == '\t' || payload[i] == '\r') {
			i++
		}
		if i < len(payload) && (payload[i] == '{' || payload[i] == '[') {
			return packSingleRowBlock(payload), nil
		}
	}

	return nil, fmt.Errorf("unknown block payload format")
}

func packSingleRowBlock(row []byte) []byte {
	out := make([]byte, 2+4+len(row))
	binary.LittleEndian.PutUint16(out[0:], 1)
	binary.LittleEndian.PutUint32(out[2:], 0)
	copy(out[6:], row)
	return out
}

func isValidBlockLayout(buf []byte) bool {
	if len(buf) < 6 {
		return false
	}
	count := int(binary.LittleEndian.Uint16(buf[0:2]))
	if count <= 0 {
		return false
	}
	headerSize := 2 + 4*count
	if headerSize > len(buf) {
		return false
	}
	prev := uint32(0)
	dataLen := uint32(len(buf) - headerSize)
	for i := 0; i < count; i++ {
		off := binary.LittleEndian.Uint32(buf[2+i*4 : 2+i*4+4])
		if i > 0 && off < prev {
			return false
		}
		if off > dataLen {
			return false
		}
		prev = off
	}
	return true
}

// ExtractRowFromDecompressed returns the specific row at index from an already decompressed block.
// It returns a copy of the data to ensure safety (so modification doesn't affect cache).
func ExtractRowFromDecompressed(decompressed []byte, index uint16) ([]byte, error) {
	if len(decompressed) < 2 {
		return nil, fmt.Errorf("invalid block: too short")
	}

	count := binary.LittleEndian.Uint16(decompressed[0:])
	if int(index) >= int(count) {
		return nil, fmt.Errorf("index %d out of bounds (count %d)", index, count)
	}

	offsetIdx := 2 + int(index)*4
	if offsetIdx+4 > len(decompressed) {
		return nil, fmt.Errorf("corrupt block header")
	}
	offset := binary.LittleEndian.Uint32(decompressed[offsetIdx:])

	var nextOffset uint32
	if int(index)+1 < int(count) {
		nextOffset = binary.LittleEndian.Uint32(decompressed[offsetIdx+4:])
	} else {
		nextOffset = uint32(len(decompressed) - (2 + 4*int(count)))
	}

	dataStart := 2 + 4*int(count)
	start := dataStart + int(offset)
	end := dataStart + int(nextOffset)

	if start > len(decompressed) || end > len(decompressed) {
		return nil, fmt.Errorf("block data out of bounds")
	}

	// Copy result to return safe byte slice
	result := make([]byte, end-start)
	copy(result, decompressed[start:end])
	return result, nil
}
