package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// MemBlock represents a collection of rows in memory, prior to compression and writing.
type MemBlock struct {
	Rows [][]byte // Each slice is a JSON byte array for a single row (e.g., `{"id":1}`).
	Size uint32   // Total size of all row data.
}

// NewMemBlock creates a new MemBlock instance.
func NewMemBlock() *MemBlock {
	return &MemBlock{
		Rows: make([][]byte, 0, 1024),
		Size: 0,
	}
}

// Append a row to the block, returns row index in block.
func (m *MemBlock) Append(row []byte) uint16 {
	m.Rows = append(m.Rows, row)
	m.Size += uint32(len(row))
	return uint16(len(m.Rows) - 1)
}

// Finalize serializes the block into the hybrid format:
// [JSON Array Payload] [Binary Offsets] [Row Count]
func (m *MemBlock) Finalize() ([]byte, error) {
	if len(m.Rows) == 0 {
		// A block with no rows should still be a valid, empty JSON array.
		return []byte("[]"), nil
	}

	buf := new(bytes.Buffer)

	// --- 1. Write Data Payload (Single JSON Array) ---
	offsets := make([]uint32, len(m.Rows))
	curr := uint32(1) // Start offset is 1 because of the leading '['

	buf.WriteByte('[') // Start of JSON array

	for i, r := range m.Rows {
		offsets[i] = curr
		buf.Write(r)
		curr += uint32(len(r))

		// Add a comma separator, UNLESS it's the last row
		if i < len(m.Rows)-1 {
			buf.WriteByte(',')
			curr++ // Account for the comma byte
		}
	}

	buf.WriteByte(']') // End of JSON array (Final byte of the JSON payload)

	// --- 2. Write Binary Index (Offsets - uint32) ---
	// The binary index begins immediately after the closing ']'
	for _, off := range offsets {
		if err := binary.Write(buf, binary.LittleEndian, off); err != nil {
			return nil, err
		}
	}

	// --- 3. Write Row Count (Index Anchor - uint16) ---
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(m.Rows))); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Iterator reconstructs an iterator over a finalized block payload,
// extracting the necessary binary index from the end of the data.
func (m *MemBlock) Iterator(data []byte) *MemBlockIterator {
	if len(data) < 2 {
		return nil // Cannot even read the row count
	}

	// 1. Read the Row Count (last 2 bytes)
	rowCount := binary.LittleEndian.Uint16(data[len(data)-2:])

	// 2. Calculate the size and start of the Offsets Array
	// indexSize = 2 bytes (count) + rowCount * 4 bytes (for uint32 offsets)
	indexSize := 2 + int(rowCount)*4
	offsetTableStart := len(data) - indexSize

	if offsetTableStart < 0 || len(data) < indexSize {
		// Corrupt block: data size is smaller than the index size implies
		return nil
	}

	// 3. Extract Offsets
	offsets := make([]uint32, rowCount)
	for i := 0; i < int(rowCount); i++ {
		// Read 4 bytes at a time for each uint32 offset
		offsetBytes := data[offsetTableStart+i*4 : offsetTableStart+(i+1)*4]
		offsets[i] = binary.LittleEndian.Uint32(offsetBytes)
	}

	return &MemBlockIterator{
		data:    data[:offsetTableStart], // This is the pure JSON array payload area
		offsets: offsets,
		index:   0,
	}
}

// MemBlockIterator is used for sequential reading of a decompressed block.
type MemBlockIterator struct {
	data    []byte   // The JSON array payload: `[{"id":1}, {"id":2}, ...]`
	offsets []uint32 // Offsets to the start of each row's JSON object.
	index   int
}

// Next returns the byte slice for the next single JSON row object.
// It uses the pre-calculated offsets to find the start and end of the row's JSON object.
func (it *MemBlockIterator) Next() ([]byte, error) {
	if it.index >= len(it.offsets) {
		return nil, io.EOF // End of block
	}

	start := it.offsets[it.index]
	var end uint32

	if it.index == len(it.offsets)-1 {
		// This is the last row, its data goes up to the closing ']'
		// Note: The total data length includes the '[' at index 0 and the final ']'
		end = uint32(len(it.data)) - 1 // End before the closing ']'
	} else {
		// The end of this row is the offset of the next row MINUS the comma separator.
		end = it.offsets[it.index+1] - 1 // -1 to exclude the comma
	}

	// Safety check to prevent panics and handle corruption
	if start >= end || end > uint32(len(it.data)) {
		return nil, fmt.Errorf("corrupt block: invalid offsets for row %d (start: %d, end: %d)", it.index, start, end)
	}

	// The slice includes the full JSON object (e.g., `{"id":1}`)
	row := it.data[start:end]
	it.index++
	return row, nil
}

// Lookup performs a direct O(1) lookup for a row by its index within the block.
func (it *MemBlockIterator) Lookup(rowIndex uint16) ([]byte, error) {
	idx := int(rowIndex)
	if idx >= len(it.offsets) {
		return nil, fmt.Errorf("row index %d out of bounds for block with %d rows", idx, len(it.offsets))
	}

	start := it.offsets[idx]
	var end uint32

	if idx == len(it.offsets)-1 {
		// Last row: ends right before the final ']'
		end = uint32(len(it.data)) - 1
	} else {
		// The end of this row is the start of the next row, minus the comma separator
		end = it.offsets[idx+1] - 1
	}

	// Sanity check
	if start >= end || end > uint32(len(it.data)) {
		return nil, fmt.Errorf("corrupt block: invalid offsets for row %d (start: %d, end: %d)", idx, start, end)
	}

	// Returns the isolated JSON object slice
	return it.data[start:end], nil
}
