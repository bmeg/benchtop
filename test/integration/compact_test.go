package test

import (
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/test/fixtures"
	"github.com/bmeg/benchtop/util"
)

func TestCompact(t *testing.T) {
	dbname := "test_compact.data" + util.RandomString(5)

	dr, err := benchtop.NewBSONDriver(dbname)
	if err != nil {
		t.Fatal(err)
	}

	ts, err := dr.New("table_1", []benchtop.ColumnDef{
		{Name: "field1", Type: benchtop.Double},
		{Name: "name", Type: benchtop.String},
	})
	if err != nil {
		t.Fatal(err)
	}

	for k, r := range fixtures.ScanData {
		err := ts.Add([]byte(k), r)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = ts.Delete([]byte("key4"))
	if err != nil {
		t.Fatal(err)
	}

	// Get the file size before compaction
	beforeStat, err := os.Stat(dbname + "/TABLES/table_1")
	if err != nil {
		t.Fatal(err)
	}
	beforeSize := beforeStat.Size()

	err = ts.Compact()
	if err != nil {
		t.Fatal(err)
	}

	afterStat, err := os.Stat(dbname + "/TABLES/table_1")
	if err != nil {
		t.Fatal(err)
	}
	afterSize := afterStat.Size()

	if afterSize >= beforeSize {
		t.Errorf("Expected file size to decrease after compaction, but it remained the same or increased: before=%d, after=%d", beforeSize, afterSize)
	} else {
		t.Logf("size before=%d, after=%d", beforeSize, afterSize)
	}

	testChan, err := ts.Scan(nil, "field1", "name", "keyName")
	if err != nil {
		t.Error(err)
	}

	t.Log("elems after")
	for elem := range testChan {
		t.Log(elem)
	}

	val, err := ts.Get([]byte("key8"))
	if err != nil {
		t.Error(err)
	}

	if val["keyName"] != "key8" {
		t.Errorf("fetched key8 but got %s instead", val["keyName"])
	}

	defer dr.Close()
	os.RemoveAll(dbname)
}
