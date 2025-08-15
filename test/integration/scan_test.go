package test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/benchtop/test/fixtures"

	"github.com/bmeg/benchtop/util"
)

type FieldFilters []benchtop.FieldFilter

func (ff FieldFilters) Matches(row any) bool {
	rowData, ok := row.(map[string]any)
	if !ok {
		return false
	}
	for _, filter := range ff {
		fieldValue, ok := rowData[filter.Field]
		if !ok {
			return false
		}
		switch filter.Operator {
		case benchtop.OP_EQ:
			if fmt.Sprintf("%v", fieldValue) != fmt.Sprintf("%v", filter.Value) {
				return false
			}
		case benchtop.OP_STARTSWITH:
			strVal, ok := fieldValue.(string)
			if !ok {
				return false
			}
			filterVal, ok := filter.Value.(string)
			if !ok {
				return false
			}
			if !strings.HasPrefix(strVal, filterVal) {
				return false
			}
		case benchtop.OP_GT:
			val1, ok1 := fieldValue.(float64)
			val2, ok2 := filter.Value.(float64)
			if !ok1 || !ok2 {
				// Handle type mismatch, maybe return false or an error
				return false
			}
			if val1 <= val2 {
				return false // Does not match the "greater than" condition
			}
		}
	}
	return true
}

func (ff FieldFilters) IsNoOp() bool {
	return len(ff) == 0
}

func (ff FieldFilters) RequiredFields() []string {
	fields := make([]string, len(ff))
	for i, filter := range ff {
		fields[i] = filter.Field
	}
	return fields
}

func TestScan(t *testing.T) {
	dbname := "test.data" + util.RandomString(5)
	defer os.RemoveAll(dbname)

	dr, err := bsontable.NewBSONDriver(dbname)
	if err != nil {
		t.Error(err)
	}

	ts, err := dr.New("table_1", []benchtop.ColumnDef{
		{Key: "field1", Type: benchtop.Double},
		{Key: "name", Type: benchtop.String},
	})
	if err != nil {
		t.Error(err)
	}

	bT, _ := ts.(*bsontable.BSONTable)
	for k, r := range fixtures.ScanData {
		loc, err := bT.AddRow(benchtop.Row{Id: []byte(k), Data: r})
		if err != nil {
			t.Error(err)
		}
		if loc.Offset == 0 || loc.Size == 0 {
			t.Error(fmt.Errorf("expecting Offset and Size to be populated but got %d and %d instead", loc.Offset, loc.Size))
		}
		err = bT.AddTableEntryInfo(nil, []byte(k), *loc)
		if err != nil {
			t.Error(err)
		}
	}

	filters1 := FieldFilters{benchtop.FieldFilter{Field: "name", Operator: benchtop.OP_EQ, Value: "alice"}}
	lenscanChan1 := 0
	for elem := range bT.Scan(true, filters1) {
		lenscanChan1++
		t.Log("scanChan: ", elem)
		if elem.(map[string]any)["name"] != "alice" {
			t.Errorf("expecting chan of len 1 with value name:alice got %v", elem)
		}
		if _, ok := elem.(map[string]any)["_key"]; ok {
			t.Errorf("specified no key to be returned but returned key anyway")
		}
	}
	if lenscanChan1 != 1 {
		t.Errorf("expected 1 element, but got %d", lenscanChan1)
	}

	// Second test case: "field1" == 0.2
	filters2 := FieldFilters{benchtop.FieldFilter{Field: "field1", Operator: benchtop.OP_EQ, Value: 0.2}}
	scanChan2 := bT.Scan(true, filters2)

	for elem := range scanChan2 {
		t.Log("scanChantwo: ", elem)
		data, ok := elem.(map[string]any)
		if !ok {
			t.Errorf("expected map[string]any, but got %T", elem)
			continue
		}
		if data["field1"] != 0.2 {
			t.Errorf("expecting chan of len 1 with value field:0.2 got %v", elem)
		}
		if key, ok := data["_key"]; ok {
			if key == "" {
				t.Errorf("specified key to be returned but got an empty string")
			}
		}
	}

	// Third test case: "field1" > 0.2
	filters3 := FieldFilters{benchtop.FieldFilter{Field: "field1", Operator: benchtop.OP_GT, Value: 0.2}}
	scanChan3 := bT.Scan(true, filters3)

	scanChanLen3 := 0
	for elem := range scanChan3 {
		t.Log("scanChanthree: ", elem)
		scanChanLen3++
		data, ok := elem.(map[string]any)
		if !ok {
			t.Errorf("expected map[string]any, but got %T", elem)
			continue
		}
		if key, ok := data["_key"]; ok {
			if key == "" {
				t.Errorf("specified key to be returned but got an empty string")
			}
		}
	}
	if scanChanLen3 != 6 {
		t.Errorf("Expecting 6 items returned but got %d", scanChanLen3)
	}

	err = bT.DeleteRow([]byte("key4"))
	if err != nil {
		t.Error(err)
	}

	// Fourth test case: "name" starts with "a"
	// NOTE: You need to fix the case in your original code from "startswith" to "STARTSWITH"
	filters4 := FieldFilters{benchtop.FieldFilter{Field: "name", Operator: benchtop.OP_STARTSWITH, Value: "a"}}
	scanChan4 := bT.Scan(false, filters4)

	scanChanLen4 := 0
	for elem := range scanChan4 {
		t.Log("scanChanfour: ", elem)
		scanChanLen4++
		if key, ok := elem.(string); !ok {
			t.Errorf("specified returned key is not string %s", key)
		}
	}
	if scanChanLen4 != 1 {
		t.Errorf("Expecting only one elem after delete key4, but got %d", scanChanLen4)
	}

	defer dr.Close()
}
