package jsontable

import (
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/util"
	"github.com/stretchr/testify/assert"
)

func TestLookupTableIDCaseInsensitive(t *testing.T) {
	dbPath := "test_case_db_" + util.RandomString(5)
	defer os.RemoveAll(dbPath)

	driver, err := NewJSONDriver(dbPath)
	assert.NoError(t, err)
	defer driver.Close()

	tableName := "TestTable"
	columns := []benchtop.ColumnDef{{Key: "data"}}
	_, err = driver.New(tableName, columns)
	assert.NoError(t, err)

	// Direct match
	tid1, err := driver.LookupTableID("TestTable")
	assert.NoError(t, err)
	assert.NotZero(t, tid1)

	// Different casing
	tid2, err := driver.LookupTableID("testtable")
	assert.NoError(t, err)
	assert.Equal(t, tid1, tid2)

	tid3, err := driver.LookupTableID("TESTTABLE")
	assert.NoError(t, err)
	assert.Equal(t, tid1, tid3)
}
