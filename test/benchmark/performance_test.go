package test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable"
	jTable "github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/benchtop/test/fixtures"
	"github.com/bmeg/benchtop/util"
	"github.com/cockroachdb/pebble"
)

const (
	NumRows   = 50000
	ValueSize = 1024 // 1KB
	BatchSize = 1000
)

func setupBenchmarkDB(b *testing.B) (*jsontable.JSONDriver, *jTable.JSONTable, string) {
	dbPath := "bench_db_" + util.RandomString(5)
	_ = os.RemoveAll(dbPath) // Cleanup potential old run

	driver, err := jsontable.NewJSONDriver(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	jDriver, _ := driver.(*jsontable.JSONDriver)

	columns := []benchtop.ColumnDef{{Key: "data"}}
	tableName := "bench_table"

	tStore, err := jDriver.New(tableName, columns)
	if err != nil {
		b.Fatal(err)
	}
	table, _ := tStore.(*jTable.JSONTable)

	// Populate Data
	rows := make([]benchtop.Row, NumRows)
	for i := 0; i < NumRows; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		val := fixtures.GenerateRandomBytes(ValueSize)
		rows[i] = benchtop.Row{
			Id:      key,
			TableID: table.TableId,
			Data:    map[string]any{"data": val, "id": string(key)},
		}
	}

	// Write in batches
	for i := 0; i < NumRows; i += 1000 {
		end := i + 1000
		if end > NumRows {
			end = NumRows
		}
		batch := rows[i:end]

		// This simulates grip's batch load (conceptually)
		// But we just use direct table.AddRows which is what grip does under the hood via driver?
		// No, grip uses driver.BulkLoad or insert.
		// Let's use table.AddRows to be direct.
		locs, err := table.AddRows(batch)
		if err != nil {
			b.Fatal(err)
		}

		// Register in Driver (LocCache) mimicking grip's behavior
		pk := &pebblebulk.PebbleBulk{Batch: jDriver.Pkv.Db.NewBatch()}
		for j, loc := range locs {
			r := batch[j]
			jDriver.LocCache.Set(string(r.Id), loc)
			// Add entry info (skipped for pure read benchmark correctness, assuming we use LocCache)
		}
		pk.Batch.Commit(pebble.NoSync)
		pk.Batch.Close()
	}

	return jDriver, table, dbPath
}

func BenchmarkGetRowSequential(b *testing.B) {
	driver, table, path := setupBenchmarkDB(b)
	defer func() {
		driver.Close()
		os.RemoveAll(path)
	}()

	b.ResetTimer()

	// We will query keys sequentially
	for i := 0; i < b.N; i++ {
		idx := i % NumRows
		key := fmt.Sprintf("key_%d", idx)

		// 1. LocLookup
		loc, err := driver.LocCache.Get(context.Background(), key)
		if err != nil {
			b.Fatal(err)
		}

		// 2. Fetch
		_, err = table.GetRow(loc)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetRowsBatch(b *testing.B) {
	driver, table, path := setupBenchmarkDB(b)
	defer func() {
		driver.Close()
		os.RemoveAll(path)
	}()

	// Prepare batches of RowLocs
	// Sequential batches (simulates scanning)
	// 1000 items per batch

	var allLocs []*benchtop.RowLoc
	for i := 0; i < NumRows; i++ {
		key := fmt.Sprintf("key_%d", i)
		loc, _ := driver.LocCache.Get(context.Background(), key)
		allLocs = append(allLocs, loc)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		start := (i * BatchSize) % NumRows
		end := start + BatchSize
		if end > NumRows {
			end = NumRows
			// Wrap around handling simplified: just cap and continue
		}
		batchLocs := allLocs[start:end]

		_, errs := table.GetRows(batchLocs)
		for _, e := range errs {
			if e != nil {
				b.Fatal(e)
			}
		}
	}
}

// Mimics grip's random access pattern if IDs are random
func BenchmarkGetRowsRandomBatch(b *testing.B) {
	// Setup with random order?
	// Actually, shuffling the locs array simulates random access
	driver, table, path := setupBenchmarkDB(b)
	defer func() {
		driver.Close()
		os.RemoveAll(path)
	}()

	var allLocs []*benchtop.RowLoc
	for i := 0; i < NumRows; i++ {
		key := fmt.Sprintf("key_%d", i)
		loc, _ := driver.LocCache.Get(context.Background(), key)
		allLocs = append(allLocs, loc)
	}

	// Shuffle
	// (Skipping shuffle implementation for brevity, relying on pseudo-random access via stride)
	// Access with stride

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Create a "random" batch by taking every 7th element wrapping around
		batchLocs := make([]*benchtop.RowLoc, BatchSize)
		for j := 0; j < BatchSize; j++ {
			idx := ((i * BatchSize) + (j * 17)) % NumRows
			batchLocs[j] = allLocs[idx]
		}

		_, errs := table.GetRows(batchLocs)
		for _, e := range errs {
			if e != nil {
				b.Fatal(e)
			}
		}
	}
}
