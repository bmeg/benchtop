package test

import (
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/test/fixtures"

	"github.com/bmeg/benchtop/util"
)

func TestScan(t *testing.T) {
	dbname := "test.data" + util.RandomString(5)

	dr, err := benchtop.NewBSONDriver(dbname)
	if err != nil {
		t.Error(err)
	}

	ts, err := dr.New("table_1", []benchtop.ColumnDef{
		{Name: "field1", Type: benchtop.Double},
		{Name: "name", Type: benchtop.String},
	})
	if err != nil {
		t.Error(err)
	}

	for k, r := range fixtures.ScanData {
		err := ts.Add([]byte(k), r)
		if err != nil {
			t.Error(err)
		}
	}

	lenscanChan := 0
	scanChan, err := ts.Scan([]benchtop.FieldFilter{benchtop.FieldFilter{Field: "name", Operator: "==", Value: "alice"}}, "name", "field1", "keyName")
	if err != nil {
		t.Error(err)
	}
	for elem := range scanChan {
		lenscanChan++
		t.Log("scanChan: ", elem)
		if elem["name"] != "alice" {
			t.Errorf("expecting chan of len 1 with value name:alice got %s", elem)
		}
	}

	scanChantwo, err := ts.Scan([]benchtop.FieldFilter{benchtop.FieldFilter{Field: "field1", Operator: "==", Value: 0.2}}, "name", "field1", "keyName")
	if err != nil {
		t.Error(err)
	}
	for elem := range scanChantwo {
		t.Log("scanChantwo: ", elem)
		if elem["field1"] != 0.2 {
			t.Errorf("expecting chan of len 1 with value field:0.2 got %s", elem)
		}
	}

	scanChanthree, err := ts.Scan([]benchtop.FieldFilter{benchtop.FieldFilter{Field: "field1", Operator: ">", Value: 0.2}}, "name", "field1", "keyName")
	if err != nil {
		t.Error(err)
	}
	scanChanLen := 0
	for elem := range scanChanthree {
		t.Log("scanChanthree: ", elem)
		scanChanLen++
	}
	if scanChanLen != 6 {
		t.Error("Expecting 7 items returned but got ", scanChanLen)
	}

	err = ts.Delete([]byte("key4"))
	if err != nil {
		t.Error(err)
	}

	scanChanfour, err := ts.Scan([]benchtop.FieldFilter{benchtop.FieldFilter{Field: "name", Operator: "startswith", Value: "a"}}, "name", "field1", "keyName")
	if err != nil {
		t.Error(err)
	}
	scanChanLen = 0
	for elem := range scanChanfour {
		t.Log("scanChanfour: ", elem)
		scanChanLen++
	}
	if scanChanLen != 1 {
		t.Error("Expecting only one elem after delete key4")
	}

	ts.Compact()
	defer dr.Close()
	os.RemoveAll(dbname)
}
