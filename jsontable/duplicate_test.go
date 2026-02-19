package jsontable

import (
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/util"
	"github.com/stretchr/testify/assert"
)

func TestBulkLoadDuplicatePrevention(t *testing.T) {
	dbPath := "test_duplicate_db_" + util.RandomString(5)
	defer os.RemoveAll(dbPath)

	driver, err := NewJSONDriver(dbPath)
	assert.NoError(t, err)
	defer driver.Close()

	tableName := "v_Person"
	columns := []benchtop.ColumnDef{{Key: "name"}}
	tStore, err := driver.New(tableName, columns)
	assert.NoError(t, err)

	tid, _ := driver.LookupTableID(tableName)

	// 1. First bulk load
	rowCount := 10
	ch1 := make(chan *benchtop.Row, rowCount)
	go func() {
		defer close(ch1)
		for i := 0; i < rowCount; i++ {
			row := benchtop.Row{
				Id:      []byte(fmt.Sprintf("person_%d", i)),
				TableID: tid,
				Data:    map[string]any{"name": fmt.Sprintf("Person %d", i)},
			}
			ch1 <- &row
		}
	}()
	err = driver.BulkLoad(tid, ch1)
	assert.NoError(t, err)

	// Verify count
	count := 0
	for range tStore.ScanDoc(nil) {
		count++
	}
	assert.Equal(t, rowCount, count)

	// 2. Second bulk load with same IDs
	ch2 := make(chan *benchtop.Row, rowCount)
	go func() {
		defer close(ch2)
		for i := 0; i < rowCount; i++ {
			row := benchtop.Row{
				Id:      []byte(fmt.Sprintf("person_%d", i)),
				TableID: tid,
				Data:    map[string]any{"name": fmt.Sprintf("Person %d Duplicate", i)},
			}
			ch2 <- &row
		}
	}()
	err = driver.BulkLoad(tid, ch2)
	assert.NoError(t, err)

	// Verify count is STILL rowCount (no duplicates)
	count2 := 0
	for range tStore.ScanDoc(nil) {
		count2++
	}
	assert.Equal(t, rowCount, count2, "Should not have added duplicate person entries")
}
