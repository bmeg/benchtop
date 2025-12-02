package test

import (
	"context"
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/filters"
	"github.com/bmeg/benchtop/jsontable"
	"github.com/bmeg/benchtop/jsontable/table"
	jTable "github.com/bmeg/benchtop/jsontable/table"
	"github.com/bytedance/sonic"
	"github.com/bytedance/sonic/ast"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/bmeg/benchtop/test/fixtures"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"

	"github.com/bmeg/benchtop/util"
)

type FieldFilters []filters.FieldFilter

func localMatchesHasExpression(row []byte, stmt *gripql.HasExpression, tableName string) bool {
	switch stmt.Expression.(type) {
	case *gripql.HasExpression_Condition:
		cond := stmt.GetCondition()
		var lookupVal any
		switch cond.Key {
		case "_label":
			lookupVal = tableName[2:]
		case "_id":
			node, err := sonic.Get(row, []any{"1"}...)
			if err != nil {
				if err != ast.ErrNotExist {
					log.Errorf("Sonic Fetch err for path 1 on doc %#v: %v", string(row), err)
				}
				return false
			}
			lookupVal, err = node.Interface()
			if err != nil {
				log.Errorf("Error unmarshaling node: %v", err)
				return false
			}
		default: // Handles all other keys (e.g., standard properties)
			pathArr, err := table.ConvertJSONPathToArray(cond.Key)
			if err != nil {
				log.Errorf("Error converting JSON path: %v", err)
				return false
			}
			node, err := sonic.Get(row, pathArr...)
			if err != nil {
				if err != ast.ErrNotExist {
					log.Errorf("Sonic Fetch err for path: %s on doc %#v: %v", pathArr, string(row), err)
					return false
				}
				lookupVal = nil
			} else {
				lookupVal, err = node.Interface()
				if err != nil {
					log.Errorf("Error unmarshaling node: %v", err)
					return false
				}
			}
		}

		// ApplyFilterCondition must be accessible via your bFilters import
		//
		return filters.ApplyFilterCondition(
			lookupVal,
			&filters.FieldFilter{
				Operator: cond.Condition,
				Field:    cond.Key,
				Value:    cond.Value.AsInterface(),
			},
		)

	case *gripql.HasExpression_And:
		for _, e := range stmt.GetAnd().Expressions {
			if !localMatchesHasExpression(row, e, tableName) {
				return false
			}
		}
		return true

	case *gripql.HasExpression_Or:
		for _, e := range stmt.GetOr().Expressions {
			if localMatchesHasExpression(row, e, tableName) {
				return true
			}
		}
		return false

	case *gripql.HasExpression_Not:
		return !localMatchesHasExpression(row, stmt.GetNot(), tableName)

	default:
		log.Errorf("unknown where expression type: %T", stmt.Expression)
		return false
	}
}

func (ff FieldFilters) Matches(row []byte, tableStr string) bool {
	if len(ff) == 0 {
		return true
	}

	expressions := make([]*gripql.HasExpression, 0, len(ff))

	for _, filter := range ff {
		// NOTE: Since your original test code used filter.Value directly,
		// we'll convert it to *structpb.Value. You need to import this:
		// "google.golang.org/protobuf/types/known/structpb"
		valuePB, err := structpb.NewValue(filter.Value)
		if err != nil {
			return false
		}

		condition := &gripql.HasExpression_Condition{
			Condition: &gripql.HasCondition{
				Key:       filter.Field,
				Condition: filter.Operator,
				Value:     valuePB,
			},
		}

		expressions = append(expressions, &gripql.HasExpression{
			Expression: condition,
		})
	}

	// Combine all conditions into a single AND expression
	hasExpr := &gripql.HasExpression{
		Expression: &gripql.HasExpression_And{
			And: &gripql.HasExpressionList{
				Expressions: expressions,
			},
		},
	}
	return localMatchesHasExpression(row, hasExpr, tableStr)
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

	ts, err := dr.New("table_1", []benchtop.ColumnDef{
		{Key: "field1"},
		{Key: "name"},
	})
	if err != nil {
		t.Error(err)
	}

	jT, _ := ts.(*jTable.JSONTable)
	jDr, _ := dr.(*jsontable.JSONDriver)

	for k, r := range fixtures.ScanData {
		loc, err := jT.AddRow(benchtop.Row{Id: []byte(k), Data: r})
		if err != nil {
			t.Error(err)
		}
		/*if loc.Offset == 0 || loc.Size == 0 {
		t.Error(fmt.Errorf("expecting Offset and Size to be populated but got %d and %d instead", loc.Offset, loc.Size))
		}*/

		err = jDr.AddTableEntryInfo(nil, []byte(k), loc)
		if err != nil {
			t.Error(err)
		}

		jDr.LocCache.Set(k, loc)
	}

	filters1 := FieldFilters{filters.FieldFilter{Field: "name", Operator: gripql.Condition_EQ, Value: "alice"}}
	lenscanChan1 := 0
	for elem := range jT.ScanDoc(filters1) {
		lenscanChan1++
		t.Log("scanChan: ", elem)
		if elem["name"] != "alice" {
			t.Errorf("expecting chan of len 1 with value name:alice got %v", elem)
		}
		if _, ok := elem["_key"]; ok {
			t.Errorf("specified no key to be returned but returned key anyway")
		}
	}
	if lenscanChan1 != 1 {
		t.Errorf("expected 1 element, but got %d", lenscanChan1)
	}

	// Second test case: "field1" == 0.2
	for elem := range jT.ScanDoc(
		FieldFilters{filters.FieldFilter{
			Field: "field1", Operator: gripql.Condition_EQ, Value: 0.2},
		},
	) {
		t.Log("scanChantwo: ", elem)
		if elem["field1"].(float64) != 0.2 {
			t.Errorf("expecting chan of len 1 with value field:0.2 got %v", elem)
		}
		if key, ok := elem["_id"]; ok {
			if key == "" {
				t.Errorf("specified key to be returned but got an empty string")
			}
		}
	}

	// Third test case: "field1" > 0.2
	filters3 := FieldFilters{filters.FieldFilter{Field: "field1", Operator: gripql.Condition_GT, Value: 0.2}}
	scanChan3 := jT.ScanDoc(filters3)

	scanChanLen3 := 0
	for elem := range scanChan3 {
		t.Log("scanChanthree: ", elem)
		scanChanLen3++
		if key, ok := elem["_key"]; ok {
			if key == "" {
				t.Errorf("specified key to be returned but got an empty string")
			}
		}
	}
	if scanChanLen3 != 6 {
		t.Errorf("Expecting 6 items returned but got %d", scanChanLen3)
	}

	loc, err := jDr.LocCache.Get(context.Background(), "key4")
	if err != nil {
		t.Error(err)
	}
	err = jT.DeleteRow(loc, []byte("key4"))
	if err != nil {
		t.Error(err)
	}

	scanChanLen4 := 0
	for elem := range jT.ScanId(
		FieldFilters{
			filters.FieldFilter{
				Field:    "name",
				Operator: gripql.Condition_EQ,
				Value:    "mnbv",
			},
		},
	) {
		t.Log("scanChanfour: ", elem)
		scanChanLen4++
	}
	if scanChanLen4 != 1 {
		t.Errorf("Expecting only one elem after delete key4, but got %d", scanChanLen4)
	}

	defer dr.Close()
}
