package table

import (
	"context" // Added
	"fmt"
	"runtime" // Added
	"strconv"
	"strings"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/block"
	"github.com/bmeg/benchtop/jsontable/storage"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"github.com/maypok86/otter/v2"
)

// Helper for cache keys
func makeCacheKey(section, offset, size uint32) string {
	// Include size to prevent collision if offsets realign but size changes (unlikely for immutable blocks but good safety)
	return fmt.Sprintf("%d:%d:%d", section, offset, size)
}

type JSONTable struct {
	Columns   []benchtop.ColumnDef
	ColumnMap map[string]int

	TableId  uint16
	Name     string
	FileName string

	Fields     map[string]struct{}
	Storage    storage.RowStorage
	BufferPool sync.Pool

	BlockCache  *otter.Cache[string, []byte]
	BlockLoader otter.LoaderFunc[string, []byte]
	LocLookup   func(id string) (*benchtop.RowLoc, error)
}

func (b *JSONTable) Close() error {
	return b.Storage.Close()
}

func (b *JSONTable) HasField(field string) bool {
	if b.Fields == nil {
		return false
	}
	_, ok := b.Fields[field]
	return ok
}

func (b *JSONTable) AddRow(elem benchtop.Row) (*benchtop.RowLoc, error) {
	locs, err := b.AddRows([]benchtop.Row{elem})
	if err != nil {
		return nil, err
	}
	return locs[0], nil
}

const BLOCK_HEADER_SIZE = 2 // uint16 count

func (b *JSONTable) AddRows(elems []benchtop.Row) ([]*benchtop.RowLoc, error) {
	if len(elems) == 0 {
		return []*benchtop.RowLoc{}, nil
	}

	// 1. Partition rows
	type indexedRow struct {
		index int
		row   benchtop.Row
	}
	byPartition := make(map[int][]indexedRow)
	for i, elem := range elems {
		pId := b.Storage.GetPartitionId(elem.Id)
		byPartition[pId] = append(byPartition[pId], indexedRow{i, elem})
	}

	results := make([]*benchtop.RowLoc, len(elems))
	var blocks [][]byte
	var blockIds [][]byte
	var blockMap []struct {
		startIndex int
		count      int
		partition  int
		rows       []indexedRow
	}

	const BATCH_SIZE = 16

	// 2. Create blocks per partition
	for pId, rows := range byPartition {
		for i := 0; i < len(rows); i += BATCH_SIZE {
			end := i + BATCH_SIZE
			if end > len(rows) {
				end = len(rows)
			}
			batch := rows[i:end]

			// Create Block using abstraction
			block := block.NewBlock(len(batch))

			for _, r := range batch {
				packed := b.PackData(r.row.Data, string(r.row.Id))
				payload, err := sonic.Marshal(packed)
				if err != nil {
					return nil, fmt.Errorf("marshal failed: %w", err)
				}
				block.Add(payload)
			}

			compressed, err := block.Serialize(&b.BufferPool)
			if err != nil {
				return nil, err
			}

			blocks = append(blocks, compressed)
			blockIds = append(blockIds, batch[0].row.Id) // Use first ID for partition routing

			blockMap = append(blockMap, struct {
				startIndex int
				count      int
				partition  int
				rows       []indexedRow
			}{
				startIndex: len(blocks) - 1,
				count:      len(batch),
				partition:  pId,
				rows:       batch,
			})
		}
	}

	// 3. Write blocks to storage
	sLocs, err := b.Storage.AddRows(blocks, blockIds)
	if err != nil {
		return nil, err
	}

	// 4. Map results
	if len(sLocs) != len(blockMap) {
		return nil, fmt.Errorf("storage returned wrong number of locations")
	}

	for i, sLoc := range sLocs {
		bInfo := blockMap[i]
		for j, r := range bInfo.rows {
			results[r.index] = &benchtop.RowLoc{
				TableId: b.TableId,
				Section: sLoc.Section,
				Offset:  sLoc.Offset,
				Size:    sLoc.Size,
				Index:   uint16(j),
			}
		}
	}

	return results, nil
}

func (b *JSONTable) GetRowLoc(id string) (*benchtop.RowLoc, error) {
	if b.LocLookup == nil {
		return nil, fmt.Errorf("LocLookup not initialized for table %s", b.Name)
	}
	return b.LocLookup(id)
}

func (b *JSONTable) GetRow(loc *benchtop.RowLoc) (map[string]any, error) {
	cacheKey := makeCacheKey(uint32(loc.Section), loc.Offset, loc.Size)

	// Use Cache with Loader (handles de-dupe / singleflight)
	decompressed, err := b.BlockCache.Get(context.Background(), cacheKey, b.BlockLoader)
	if err != nil {
		return nil, err
	}

	rowBytes, err := block.ExtractRowFromDecompressed(decompressed, loc.Index)
	if err != nil {
		return nil, err
	}

	var m RowData
	if err := sonic.Unmarshal(rowBytes, &m); err != nil {
		return nil, fmt.Errorf("unmarshal failed: %w", err)
	}

	if m.Data != nil {
		m.Data["_id"] = m.Key
	}
	return m.Data, nil
}

func (b *JSONTable) GetRows(locs []*benchtop.RowLoc, sectionID uint16) ([]map[string]any, []error) {
	results := make([]map[string]any, len(locs))
	errors := make([]error, len(locs))

	numWorkers := runtime.NumCPU()
	if numWorkers > 8 {
		numWorkers = 8
	}

	var wg sync.WaitGroup
	chunkSize := (len(locs) + numWorkers - 1) / numWorkers

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		if start >= len(locs) {
			break
		}
		end := start + chunkSize
		if end > len(locs) {
			end = len(locs)
		}

		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			for j := s; j < e; j++ {
				// Get Block via Cache (De-duping happens here via Otter)
				cacheKey := makeCacheKey(uint32(locs[j].Section), locs[j].Offset, locs[j].Size)
				decompressed, err := b.BlockCache.Get(context.Background(), cacheKey, b.BlockLoader)

				if err != nil {
					errors[j] = err
					continue
				}

				// Extract Row
				rowBytes, err := block.ExtractRowFromDecompressed(decompressed, locs[j].Index)
				if err != nil {
					errors[j] = err
					continue
				}

				var m RowData
				if err := sonic.Unmarshal(rowBytes, &m); err != nil {
					errors[j] = err
					continue
				}

				if m.Data != nil {
					m.Data["_id"] = m.Key
				}
				results[j] = m.Data
			}
		}(start, end)
	}

	wg.Wait()
	return results, errors
}

func (b *JSONTable) DeleteRow(loc *benchtop.RowLoc, id []byte) error {
	return b.Storage.MarkDelete(loc)
}

func (b *JSONTable) MarkDeleteTable(loc *benchtop.RowLoc) error {
	return b.DeleteRow(loc, nil)
}

func (b *JSONTable) ScanDoc(filter benchtop.RowFilter) chan map[string]any {
	out := make(chan map[string]any, 100)
	rawRows := b.Storage.Scan(10)

	go func() {
		defer close(out)
		for compressed := range rawRows {
			// Check if it is a block or single row
			// IterateBlock handles both (if we updated it to handle single row as well)
			// My previous edit to block.go handles single row.

			err := block.IterateBlock(compressed, &b.BufferPool, func(rowBytes []byte) bool {
				process(rowBytes, out, filter, b, nil)
				return true
			})

			if err != nil {
				log.Errorf("scan block failed: %v", err)
			}
		}
	}()
	return out
}

func (b *JSONTable) ScanDocProjected(fields []string, filter benchtop.RowFilter) chan map[string]any {
	out := make(chan map[string]any, 100)
	go func() {
		defer close(out)
		if len(fields) == 0 {
			for row := range b.ScanDoc(filter) {
				out <- row
			}
			return
		}
		for row := range b.ScanDoc(filter) {
			proj := map[string]any{}
			if id, ok := row["_id"]; ok {
				proj["_id"] = id
			}
			for _, f := range fields {
				if f == "_id" {
					continue
				}
				if v, ok := row[f]; ok {
					proj[f] = v
				}
			}
			out <- proj
		}
	}()
	return out
}

func process(rowBytes []byte, out chan map[string]any, filter benchtop.RowFilter, b *JSONTable, pool *sync.Pool) {
	// Filter logic
	if filter != nil && !filter.IsNoOp() {
		if !filter.Matches(rowBytes, b.Name) {
			return
		}
	}
	var m RowData
	if err := sonic.Unmarshal(rowBytes, &m); err != nil {
		log.Errorf("scan unmarshal failed: %v", err)
		return
	}
	if m.Data != nil {
		m.Data["_id"] = m.Key
	}
	out <- m.Data
}

func (b *JSONTable) ScanId(filter benchtop.RowFilter) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		for res := range b.ScanFull(filter) {
			// ID is in DataMap["_id"] or can be extracted from Data
			if id, ok := res.DataMap["_id"].(string); ok {
				out <- id
			}
		}
	}()
	return out
}

func (b *JSONTable) ScanFull(filter benchtop.RowFilter) chan benchtop.RowLocData {
	out := make(chan benchtop.RowLocData, 100)
	rawRows := b.Storage.ScanFull(10)

	go func() {
		defer close(out)
		for rowLocData := range rawRows {
			// rowLocData.Data is the compressed block of rows
			var rowIndex uint16 = 0
			err := block.IterateBlock(rowLocData.Data, &b.BufferPool, func(rowBytes []byte) bool {
				if filter != nil && !filter.IsNoOp() {
					if !filter.Matches(rowBytes, b.Name) {
						rowIndex++
						return true // Continue
					}
				}

				var m RowData
				if err := sonic.Unmarshal(rowBytes, &m); err != nil {
					log.Errorf("scan unmarshal failed: %v", err)
					rowIndex++
					return true
				}
				if m.Data == nil {
					m.Data = make(map[string]any)
				}
				m.Data["_id"] = m.Key

				// Create a copy of the location and set the correct row index
				loc := *rowLocData.Loc
				loc.Index = rowIndex

				out <- benchtop.RowLocData{
					Data:    rowBytes,
					DataMap: m.Data,
					Loc:     &loc,
				}
				rowIndex++
				return true
			})
			if err != nil {
				log.Errorf("scan full block failed: %v", err)
			}
		}
	}()
	return out
}

func (b *JSONTable) GetColumnDefs() []benchtop.ColumnDef {
	return b.Columns
}

func (b *JSONTable) Init(poolSize int) error {
	// Storage is already initialized by JSONDriver via ZoneManager.
	// This remains for interface compatibility but does nothing now.
	return nil
}

func ConvertJSONPathToArray(path string) ([]any, error) {
	path = strings.TrimLeft(path, "./")
	if path == "" {
		return []any{"0"}, nil // Handle empty path after trimming
	}

	result := make([]any, 1, len(path)/2+1)
	result[0] = "0"
	var start int = 0
	var length int = len(path)

	for i := 0; i < length; i++ {
		char := path[i]

		switch char {
		case '.':
			// Found a dot separator. The preceding characters (if any) are a key.
			if i > start {
				token := path[start:i]
				if token != "" {
					result = append(result, token)
				}
			}
			start = i + 1 // Start the next token after the dot

		case '[':
			// Found the start of an array index. The preceding characters (if any) are a key.
			if i > start {
				token := path[start:i]
				if token != "" {
					result = append(result, token)
				}
			}

			// Look for the closing bracket
			j := i + 1
			for j < length && path[j] != ']' {
				j++
			}

			if j == length || j == i+1 {
				// Error: missing closing bracket or empty brackets '[]'
				return nil, fmt.Errorf("invalid path format: missing array closing bracket or empty index at position %d", i)
			}

			// Extract and convert the index string
			numStr := path[i+1 : j]
			index, err := strconv.Atoi(numStr)
			if err != nil {
				return nil, fmt.Errorf("invalid array index: %s", numStr)
			}
			result = append(result, index)

			// Skip past the index token, including the ']'
			i = j // Loop's i++ will make it j+1
			start = i + 1
		}
	}

	// Handle the final token if the path didn't end with a separator
	if start < length {
		token := path[start:length]
		if token != "" {
			result = append(result, token)
		}
	}

	return result, nil
}
