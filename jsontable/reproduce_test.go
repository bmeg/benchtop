package jsontable

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/benchtop/query"
	"github.com/bytedance/sonic"
	"github.com/bytedance/sonic/ast"
)

// Helper copied from filter_debug_test.go/filter.go
func parseDirectPath(path string) ([]any, bool) {
	path = strings.TrimSpace(path)
	path = strings.TrimPrefix(path, "$")
	path = strings.TrimPrefix(path, ".")
	if path == "" {
		return nil, false
	}

	parts := []any{}
	var token strings.Builder
	flushToken := func() {
		if token.Len() > 0 {
			parts = append(parts, token.String())
			token.Reset()
		}
	}

	for i := 0; i < len(path); i++ {
		ch := path[i]
		switch ch {
		case '.':
			flushToken()
		case '[':
			flushToken()
			j := i + 1
			for j < len(path) && path[j] != ']' {
				j++
			}
			if j >= len(path) || j == i+1 {
				return nil, false
			}
			idx, err := strconv.Atoi(path[i+1 : j])
			if err != nil {
				return nil, false
			}
			parts = append(parts, idx)
			i = j
		default:
			token.WriteByte(ch)
		}
	}
	flushToken()

	if len(parts) == 0 {
		return nil, false
	}
	return parts, true
}

func sonicLookup(row []byte, condKey string) any {
	if path, ok := parseDirectPath(condKey); ok {
		node, err := sonic.Get(row, path...)
		if err == nil {
			v, ierr := node.Interface()
			if ierr == nil {
				return v
			}
		}
	}

	// Legacy packed-row fallback
	pathArr, err := table.ConvertJSONPathToArray(condKey)
	if err != nil {
		return nil
	}
	node, err := sonic.Get(row, pathArr...)
	if err != nil {
		if err != ast.ErrNotExist {
			// log.Debugf("Sonic fetch error: %v", err)
		}
		return nil
	}
	v, ierr := node.Interface()
	if ierr != nil {
		return nil
	}
	return v
}

// MockFilter implements benchtop.RowFilter using REAL sonic lookup logic
type MockFilter struct {
	NoOp  bool
	Key   string
	Value string
}

func (m *MockFilter) Matches(row []byte, tableName string) bool {
	// Simulate GripQLFilter.Matches logic
	val := sonicLookup(row, m.Key)
	if s, ok := val.(string); ok && s == m.Value {
		return true
	}
	return false
}

func (m *MockFilter) IsNoOp() bool {
	return m.NoOp
}

func (m *MockFilter) GetFilter() any {
	return nil
}

func (m *MockFilter) RequiredFields() []string {
	if m.Key != "" {
		return []string{m.Key}
	}
	return nil
}

func TestReproduceQueryIssues(t *testing.T) {
	// Setup temp dir
	tmpDir, err := os.MkdirTemp("", "jsontable_repro")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize Driver
	driver, err := NewJSONDriver(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	tableName := "v_Observation"
	tblStore, err := driver.New(tableName, nil)
	if err != nil {
		t.Fatal(err)
	}
	tableID := tblStore.(*table.JSONTable).TableId

	err = driver.BulkLoad(tableID, loadRows(tableID, 100))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	// Get the table
	ts, err := driver.Get(tableID)
	if err != nil {
		t.Fatal(err)
	}
	tbl := ts.(*table.JSONTable)

	// 1. Test ScanDoc (Simulates Query 3b)
	t.Run("ScanDoc", func(t *testing.T) {
		count := 0
		for _ = range tbl.ScanDoc(nil) {
			count++
		}
		if count != 100 {
			t.Errorf("ScanDoc expected 100 rows, got %d", count)
		}
	})

	// 2. Test ScanId with nil filter (Should work)
	t.Run("ScanId_NilFilter", func(t *testing.T) {
		count := 0
		for _ = range tbl.ScanId(nil) {
			count++
		}
		if count != 100 {
			t.Errorf("ScanId_NilFilter expected 100 rows, got %d", count)
		}
	})

	// 3. Test ScanId with NoOp Filter (Simulates Query 3)
	t.Run("ScanId_NoOpFilter", func(t *testing.T) {
		f := &MockFilter{NoOp: true}
		count := 0
		for _ = range tbl.ScanId(f) {
			count++
		}
		if count != 100 {
			t.Errorf("ScanId_NoOpFilter expected 100 rows, got %d", count)
		}
	})

	// 4. Test ScanDoc with Nested Filter (Simulates Query 5)
	t.Run("ScanDoc_NestedFilter", func(t *testing.T) {
		f := &MockFilter{
			NoOp:  false,
			Key:   "component.[0].valueString",
			Value: "Post-treatment",
		}

		count := 0
		for _ = range tbl.ScanDoc(f) {
			count++
		}
		// expect 50 rows
		if count != 50 {
			t.Errorf("ScanDoc_NestedFilter expected 50 rows, got %d", count)
		}
	})

	// 5. Test ScanFull directly
	t.Run("ScanFull_NoOp", func(t *testing.T) {
		f := &MockFilter{NoOp: true}
		count := 0
		for res := range tbl.ScanFull(f) {
			if res.DataMap["_id"] == "" {
				t.Error("ScanFull returned empty _id")
			}
			count++
		}
		if count != 100 {
			t.Errorf("ScanFull expected 100 rows, got %d", count)
		}
	})

	// 6. Test RowIdsByHas (Simulates V().Has(...))
	t.Run("RowIdsByHas_Eq", func(t *testing.T) {
		count := 0
		// Look for "Post-treatment" in "component.[0].valueString"
		// Note: Field name stored in index includes path?
		// In loadRows: "component" is array of map.
		// Does benchmark store complex paths in fields?
		// fields.go scans:
		// fieldValue := tpath.PathLookup(r.DataMap, field)
		// So if we ask for field "component.[0].valueString", it should work if we index it via AddField.
		// But here we are just scanning. RowIdsByHas defaults to scan if index missing.

		for range driver.RowIdsByHas("component.[0].valueString", "Post-treatment", query.EQ) {
			count++
		}
		if count != 50 {
			t.Errorf("RowIdsByHas_Eq expected 50 rows, got %d", count)
		}
	})

	// 7. Test RowIdsByLabelFieldValue
	t.Run("RowIdsByLabelFieldValue_Eq", func(t *testing.T) {
		count := 0
		for range driver.RowIdsByTableFieldValue(tableID, "component.[0].valueString", "Post-treatment", query.EQ) {
			count++
		}
		if count != 50 {
			t.Errorf("RowIdsByLabelFieldValue_Eq expected 50 rows, got %d", count)
		}
	})
}

func loadRows(tableID uint16, count int) chan *benchtop.Row {
	out := make(chan *benchtop.Row)
	go func() {
		defer close(out)
		for i := 0; i < count; i++ {
			id := fmt.Sprintf("row_%d", i)

			compVal := "Pre-treatment"
			if i%2 == 0 {
				compVal = "Post-treatment"
			}

			data := map[string]any{
				"data": fmt.Sprintf("value_%d", i),
				"component": []any{
					map[string]any{
						"valueString": compVal,
					},
				},
			}
			out <- &benchtop.Row{
				Id:      []byte(id),
				TableID: tableID,
				Data:    data,
			}
		}
	}()
	return out
}
