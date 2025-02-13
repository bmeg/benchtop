package test

import (
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/util"
)

var scanData = map[string]map[string]any{
	"key1": {
		"field1": 0.1,
		"name":   "alice",
		"other":  "other data",
		"isEven": true,
	},
	"key2": {
		"field1": 0.2,
		"name":   "bob",
	},
	"key3": {
		"field1": 0.3,
		"name":   "chelsie",
	},
	"key4": {
		"field1": 0.4,
		"name":   "asfg",
	},
	"key5": {
		"field1": 0.5,
		"name":   "hgfd",
	},
	"key6": {
		"field1": 0.7,
		"name":   "qwer",
	},
	"key7": {
		"field1": 0.8,
		"name":   "zxcv",
	},
	"key8": {
		"field1": 0.9,
		"name":   "mnbv",
	},
}

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

	for k, r := range scanData {
		err := ts.Add([]byte(k), r)
		if err != nil {
			t.Error(err)
		}
	}

	scanChan, err := ts.Scan([]benchtop.FieldFilter{benchtop.FieldFilter{Field: "name", Operator: "==", Value: "alice"}}, "name", "field1", "keyName")
	if err != nil {
		t.Error(err)
	}
	for elem := range scanChan {
		t.Log("scanChan: ", elem)
	}

	scanChantwo, err := ts.Scan([]benchtop.FieldFilter{benchtop.FieldFilter{Field: "field1", Operator: "==", Value: 0.2}}, "name", "field1", "keyName")
	if err != nil {
		t.Error(err)
	}
	for elem := range scanChantwo {
		t.Log("scanChantwo: ", elem)
	}

	scanChanthree, err := ts.Scan([]benchtop.FieldFilter{benchtop.FieldFilter{Field: "field1", Operator: ">", Value: 0.2}}, "name", "field1", "keyName")
	if err != nil {
		t.Error(err)
	}
	for elem := range scanChanthree {
		t.Log("scanChanthree: ", elem)
	}

	scanChanfour, err := ts.Scan([]benchtop.FieldFilter{benchtop.FieldFilter{Field: "name", Operator: "startswith", Value: "a"}}, "name", "field1", "keyName")
	if err != nil {
		t.Error(err)
	}
	for elem := range scanChanfour {
		t.Log("scanChanfour: ", elem)
	}

	ts.Compact()
	dr.Close()
	os.RemoveAll(dbname)
}
