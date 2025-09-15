package test

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/filters"
	"github.com/bmeg/benchtop/jsontable"
	jTable "github.com/bmeg/benchtop/jsontable/table"

	"github.com/bmeg/benchtop/test/fixtures"
	"github.com/bmeg/grip/gripql"

	"github.com/bmeg/benchtop/util"
)

type FieldFilters []filters.FieldFilter

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
		case gripql.Condition_EQ:
			if fmt.Sprintf("%v", fieldValue) != fmt.Sprintf("%v", filter.Value) {
				return false
			}
		case gripql.Condition_GT:
			val1, ok1 := fieldValue.(float64)
			val2, ok2 := filter.Value.(float64)
			if !ok1 || !ok2 {
				// Handle type mismatch, maybe return false or an error
				return false
			}
			if val1 <= val2 {
				return false // Does not match the "greater than" condition
			}

		case gripql.Condition_CONTAINS:
			found := false
			switch val := filter.Value.(type) {
			case []any:
				for _, v := range val {
					if reflect.DeepEqual(v, fieldValue) {
						found = true
					}
				}
			case nil:
				found = false
			default:
			}
			return found

		default:
			return false
		}

	}
	return true
}

func (ff FieldFilters) IsNoOp() bool {
	return len(ff) == 0
}

func (ff FieldFilters) GetFilter() any {
	return ff
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

	dr, err := jsontable.NewJSONDriver(dbname)
	if err != nil {
		t.Error(err)
	}

	jR,_ dr.(jsontable.*JSONDriver)

	ts, err := dr.New("table_1", []benchtop.ColumnDef{
		{Key: "field1"},
		{Key: "name"},
	})
	if err != nil {
		t.Error(err)
	}

	jT, _ := ts.(*jTable.JSONTable)
	for k, r := range fixtures.ScanData {
		loc, err := jT.AddRow(benchtop.Row{Id: []byte(k), Data: r})
		if err != nil {
			t.Error(err)
		}
		if loc.Offset == 0 || loc.Size == 0 {
			t.Error(fmt.Errorf("expecting Offset and Size to be populated but got %d and %d instead", loc.Offset, loc.Size))
		}
		err = jT.AddTableEntryInfo(nil, []byte(k), loc)
		if err != nil {
			t.Error(err)
		}
	}

	filters1 := FieldFilters{filters.FieldFilter{Field: "name", Operator: gripql.Condition_EQ, Value: "alice"}}
	lenscanChan1 := 0
	for elem := range jT.Scan(true, filters1) {
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
	filters2 := FieldFilters{filters.FieldFilter{Field: "field1", Operator: gripql.Condition_EQ, Value: 0.2}}
	scanChan2 := jT.Scan(true, filters2)

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
	filters3 := FieldFilters{filters.FieldFilter{Field: "field1", Operator: gripql.Condition_GT, Value: 0.2}}
	scanChan3 := jT.Scan(true, filters3)

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

	loc, err := jT.GetBlockPos([]byte("key4"))
	if err != nil {
		t.Error(err)
	}
	err = jT.DeleteRow(loc, []byte("key4"))
	if err != nil {
		t.Error(err)
	}

	// Fourth test case: "name" starts with "a"
	// NOTE: You need to fix the case in your original code from "startswith" to "STARTSWITH"
	filters4 := FieldFilters{filters.FieldFilter{Field: "name", Operator: gripql.Condition_CONTAINS, Value: []any{"mnbv"}}}
	scanChan4 := jT.Scan(false, filters4)

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
