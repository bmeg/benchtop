package filters_test

import (
	"fmt"
	"testing"

	"github.com/bmeg/benchtop/filters"
	"github.com/bmeg/benchtop/query"
)

func TestApplyFilterCondition(t *testing.T) {
	fmt.Println("--- testing EQ with nil ---")
	fmt.Println("nil eq nil:", filters.ApplyFilterCondition(nil, &query.FieldFilter{Operator: query.EQ, Value: nil}))
	fmt.Println("string eq nil:", filters.ApplyFilterCondition("foo", &query.FieldFilter{Operator: query.EQ, Value: nil}))

	fmt.Println("--- testing NEQ with nil ---")
	fmt.Println("nil neq nil:", filters.ApplyFilterCondition(nil, &query.FieldFilter{Operator: query.NEQ, Value: nil}))
	fmt.Println("string neq nil:", filters.ApplyFilterCondition("foo", &query.FieldFilter{Operator: query.NEQ, Value: nil}))

	fmt.Println("--- testing WITHOUT ---")
	fmt.Println("int without [0]:", filters.ApplyFilterCondition(1, &query.FieldFilter{Operator: query.WITHOUT, Value: []any{0}}))
	fmt.Println("string without [brown]:", filters.ApplyFilterCondition("blue", &query.FieldFilter{Operator: query.WITHOUT, Value: []any{"brown"}}))
}
