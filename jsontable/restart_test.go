package jsontable

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/table"
)

func TestRestartPersistence(t *testing.T) {
	// Setup temp dir
	tmpDir, err := os.MkdirTemp("", "jsontable_restart")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	tableName := "v_Person"
	rowCount := 50000

	// 1. Initialize Driver and Load Data
	{
		driver, err := NewJSONDriver(tmpDir)
		if err != nil {
			t.Fatal(err)
		}

		tblStore, err := driver.New(tableName, nil)
		if err != nil {
			t.Fatal(err)
		}
		tableID := tblStore.(*table.JSONTable).TableId

		err = driver.BulkLoad(tableID, loadRowsHelper(tableID, rowCount))
		if err != nil {
			t.Fatal(err)
		}
		// Allow async writes to flush? BulkLoad should handle it but let's be safe
		time.Sleep(100 * time.Millisecond)

		// Verify initial count
		ts, err := driver.Get(tableID)
		if err != nil {
			t.Fatal(err)
		}
		tbl := ts.(*table.JSONTable)

		count := 0
		for _ = range tbl.ScanDoc(nil) {
			count++
		}
		if count != rowCount {
			t.Errorf("Initial scan expected %d rows, got %d", rowCount, count)
		}

		driver.Close()
	}

	// 2. Restart Driver and Verify Data
	{
		driver, err := NewJSONDriver(tmpDir)
		if err != nil {
			t.Fatal(err)
		}
		defer driver.Close()

		// Verify table exists
		tableID, err := driver.LookupTableID(tableName)
		if err != nil {
			t.Fatalf("Failed to lookup table ID after restart: %v", err)
		}
		ts, err := driver.Get(tableID)
		if err != nil {
			t.Fatalf("Failed to get table after restart: %v", err)
		}
		tbl := ts.(*table.JSONTable)

		count := 0
		for _ = range tbl.ScanDoc(nil) {
			count++
		}
		if count != rowCount {
			t.Errorf("Post-restart scan expected %d rows, got %d", rowCount, count)
		}

		// 3. Verify Random Access (GetRow equivalent)
		// Pick a few IDs
		idsToCheck := []string{"row_0", "row_50", "row_9999"}
		for _, id := range idsToCheck {
			loc, err := tbl.LocLookup(id)
			if err != nil {
				t.Errorf("LocLookup failed for %s: %v", id, err)
				continue
			}
			if loc == nil {
				t.Errorf("LocLookup returned nil for %s", id)
				continue
			}

			// Try to read via Storage
			data, err := tbl.Storage.Get(loc)
			if err != nil {
				t.Errorf("Storage.Get failed for %s (loc=%v): %v", id, loc, err)
				continue
			}
			if len(data) == 0 {
				t.Errorf("Storage.Get returned empty data for %s", id)
			}
		}
	}
}

func loadRowsHelper(tableID uint16, count int) chan *benchtop.Row {
	out := make(chan *benchtop.Row)
	go func() {
		defer close(out)
		for i := 0; i < count; i++ {
			id := fmt.Sprintf("row_%d", i)
			data := map[string]any{
				"data": fmt.Sprintf("value_%d", i),
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
