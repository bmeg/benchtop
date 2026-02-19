package jsontable

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStressPersistence writes enough data to trigger section growth and rotation,
// then restarts the driver to verify persistence.
func TestStressPersistence(t *testing.T) {
	dir, err := os.MkdirTemp("", "jsontable_stress")
	require.NoError(t, err)
	defer os.RemoveAll(dir) // Clean up

	tableName := "t_stress"
	columns := []benchtop.ColumnDef{{Key: "data"}}

	// 1. Initial Write
	driver, err := NewJSONDriver(dir)
	require.NoError(t, err)

	// Write enough data to trigger growth (16MB increment) and rotation (65MB max)
	// We'll write 100MB of data total.
	// Each row ~1KB. So 100,000 rows.
	rowCount := 100000
	payloadSize := 1000 // 1KB payload

	// Generate heavy payload
	heavyPayload := make([]byte, payloadSize)
	rand.Read(heavyPayload)

	loadRowsHelper := func(tableID uint16, count int) chan *benchtop.Row {
		out := make(chan *benchtop.Row, 100)
		go func() {
			defer close(out)
			for i := 0; i < count; i++ {
				id := fmt.Sprintf("row_%06d", i)
				data := map[string]any{
					"data": heavyPayload, // Reuse same payload for speed, just need size
					"_id":  id,
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

	done := make(chan error)
	go func() {
		defer close(done)
		tblStore, err := driver.New(tableName, columns)
		if err != nil {
			t.Fatal(err)
		}
		tableID := tblStore.(*table.JSONTable).TableId

		err = driver.BulkLoad(tableID, loadRowsHelper(tableID, rowCount))
		if err != nil {
			t.Fatal(err)
		}
		done <- nil
	}()

	start := time.Now()
	err = <-done
	require.NoError(t, err)
	t.Logf("Wrote %d rows in %v", rowCount, time.Since(start))

	// Close driver to flush everything
	driver.Close()

	// 2. Restart and Verify
	driver2, err := NewJSONDriver(dir)
	require.NoError(t, err)
	defer driver2.Close()

	// Verify table exists
	tableID, err := driver2.LookupTableID(tableName)
	if err != nil {
		t.Fatalf("Failed to lookup table ID after restart: %v", err)
	}

	tableStore2, err := driver2.Get(tableID)
	require.NoError(t, err)

	// Spot check random rows
	checkCount := 1000
	for i := 0; i < checkCount; i++ {
		idx := rand.Intn(rowCount)
		key := fmt.Sprintf("row_%06d", idx)

		loc, err := tableStore2.GetRowLoc(key)
		require.NoError(t, err, "GetRowLoc failed for %s", key)
		require.NotNil(t, loc)

		row, err := tableStore2.GetRow(loc)
		require.NoError(t, err, "GetRow failed for %s", key)
		require.NotNil(t, row)
		assert.Equal(t, key, row["_id"])
	}

	t.Logf("Verified %d random rows successfully", checkCount)
}
