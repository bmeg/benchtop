package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type MemBlock struct {
	Rows [][]byte
	Size uint32
}

func NewMemBlock() *MemBlock {
	return &MemBlock{
		Rows: make([][]byte, 0, 1024),
		Size: 0,
	}
}

// Append a row to the block, returns row index in block
func (m *MemBlock) Append(row []byte) uint16 {
	m.Rows = append(m.Rows, row)
	m.Size += uint32(len(row))
	return uint16(len(m.Rows) - 1)
}

// Finalize serializes the block:
// [row0|row1|...|rowN|offsets[]|rowCount]
func (m *MemBlock) Finalize() ([]byte, error) {
	buf := new(bytes.Buffer)

	offsets := make([]uint32, len(m.Rows))
	curr := uint32(0)
	for i, r := range m.Rows {
		offsets[i] = curr
		buf.Write(r)
		curr += uint32(len(r))
	}

	for _, off := range offsets {
		if err := binary.Write(buf, binary.LittleEndian, off); err != nil {
			return nil, err
		}
	}

	if err := binary.Write(buf, binary.LittleEndian, uint16(len(m.Rows))); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Iterator reconstructs an iterator over a finalized block
func (m *MemBlock) Iterator(data []byte) *MemBlockIterator {
	/*if len(data) < 2 {
	return nil, fmt.Errorf("corrupt block: too small")
	}*/

	// Last 2 bytes = row count
	rowCount := binary.LittleEndian.Uint16(data[len(data)-2:])

	// Offsets array sits before rowCount
	offsetTableStart := len(data) - 2 - int(rowCount)*4
	/*if offsetTableStart < 0 {
	return nil, fmt.Errorf("corrupt block: bad offsets")
	}*/

	offsets := make([]uint32, rowCount)
	for i := 0; i < int(rowCount); i++ {
		offsets[i] = binary.LittleEndian.Uint32(data[offsetTableStart+i*4:])
	}

	return &MemBlockIterator{
		data:    data[:offsetTableStart], // row payload area
		offsets: offsets,
		index:   0,
	}
}

type MemBlockIterator struct {
	data    []byte
	offsets []uint32
	index   int
}

func (it *MemBlockIterator) Next() ([]byte, error) {
	if it.index >= len(it.offsets) {
		return nil, fmt.Errorf("end of block")
	}

	start := it.offsets[it.index]
	var end uint32
	if it.index == len(it.offsets)-1 {
		end = uint32(len(it.data))
	} else {
		end = it.offsets[it.index+1]
	}

	row := it.data[start:end]
	it.index++
	return row, nil
}
