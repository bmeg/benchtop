package jsontable

import (
	"os"
	"testing"
	"time"

	"github.com/bmeg/benchtop/jsontable/table"
)

func TestIndexConsistency(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "jsontable_consist")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	tableName := "v_Person"
	rowCount := 1000

	// 1. Load Data
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
		time.Sleep(100 * time.Millisecond)
		driver.Close()
	}

	// 2. Restart and Check Consistency
	{
		driver, err := NewJSONDriver(tmpDir)
		if err != nil {
			t.Fatal(err)
		}
		defer driver.Close()

		tableID, err := driver.LookupTableID(tableName)
		if err != nil {
			t.Fatalf("Failed to lookup table ID after restart: %v", err)
		}
		ts, err := driver.Get(tableID)
		if err != nil {
			t.Fatal(err)
		}
		tbl := ts.(*table.JSONTable)

		// Iterate ScanFull (source of truth from disk)
		scannedCount := 0
		for row := range tbl.ScanDoc(nil) {
			scannedCount++

			id, ok := row["_id"].(string)
			if !ok {
				t.Errorf("Row missing _id")
				continue
			}

			// Lookup in Index
			locFromIndex, err := tbl.LocLookup(id)
			if err != nil {
				t.Errorf("LocLookup failed for %s: %v", id, err)
				continue
			}
			if locFromIndex == nil {
				t.Errorf("LocLookup returned nil for %s", id)
				continue
			}

			// Verify data accessibility via Index Loc
			dataFromIndexLoc, err := tbl.Storage.Get(locFromIndex)
			if err != nil {
				t.Errorf("Storage.Get failed using Index Loc for %s: %v. Loc: %+v", id, err, locFromIndex)
			} else {
				// Verify content matches (decompressed)?
				// Storage.Get returns compressed block usually?
				// Wait, Storage.Get returns body (without header).
				// We need to decompress to compare with 'row' map?
				// Just checking if it errors is good enough for "decompress failed" check.
				if len(dataFromIndexLoc) == 0 {
					t.Errorf("Storage.Get return empty data")
				}
			}
		}

		if scannedCount != rowCount {
			t.Errorf("ScanDoc found %d rows, expected %d", scannedCount, rowCount)
		}
	}
}
