package test

import (
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/benchtop/test/fixtures"
	"github.com/bmeg/benchtop/util"
)

func TestCompact(t *testing.T) {
	dbname := "test_compact.data" + util.RandomString(5)
	defer os.RemoveAll(dbname)

	dr, err := bsontable.NewBSONDriver(dbname)
	if err != nil {
		t.Fatal(err)
	}

	ts, err := dr.New("table_1", []benchtop.ColumnDef{
		{Key: "field1", Type: benchtop.Double},
		{Key: "name", Type: benchtop.String},
	})
	if err != nil {
		t.Fatal(err)
	}

	loadData := make(chan benchtop.Row)
	go func() {
		for k, r := range fixtures.ScanData {
			loadData <- benchtop.Row{Id: []byte(k), Data: r}
		}
		close(loadData)
	}()
	ts.Load(loadData)

	err = ts.DeleteRow([]byte("key4"))
	if err != nil {
		t.Fatal(err)
	}

	// Get the file size before compaction
	table, err := dr.Get("table_1")
	if err != nil {
		t.Fatal(err)
	}
	beforeStat, err := os.Stat(dbname + "/TABLES/" + table.(*bsontable.BSONTable).FileName)
	if err != nil {
		t.Fatal(err)
	}
	beforeSize := beforeStat.Size()

	err = ts.Compact()
	if err != nil {
		t.Fatal(err)
	}

	afterStat, err := os.Stat(dbname + "/TABLES/" + table.(*bsontable.BSONTable).FileName)
	if err != nil {
		t.Fatal(err)
	}
	afterSize := afterStat.Size()

	if afterSize >= beforeSize {
		t.Errorf("Expected file size to decrease after compaction, but it remained the same or increased: before=%d, after=%d", beforeSize, afterSize)
	} else {
		t.Logf("size before=%d, after=%d", beforeSize, afterSize)
	}

	testChan := ts.Scan(true, nil, "field1", "name")
	if err != nil {
		t.Error(err)
	}

	t.Log("elems after")
	for elem := range testChan {
		t.Log(elem)
	}

	val, err := ts.GetRow([]byte("key8"))
	if err != nil {
		t.Error(err)
	}
	t.Log("Get key8: ", val)

	if val["name"] != "mnbv" {
		t.Errorf("fetched key8 but got name val %s instead", val["name"])
	}

	// Get another key to double check that it works
	val, err = ts.GetRow([]byte("key7"))
	if err != nil {
		t.Error(err)
	}
	t.Log("Get key7: ", val)

	if val["name"] != "zxcv" {
		t.Errorf("fetched key7 but got name val %s instead", val["name"])
	}

	ts.Compact()
	defer dr.Close()
}
