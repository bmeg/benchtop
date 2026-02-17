package query

import "github.com/bmeg/grip/gripql"

type Condition = gripql.Condition

const (
	EQ       Condition = gripql.Condition_EQ
	NEQ      Condition = gripql.Condition_NEQ
	GT       Condition = gripql.Condition_GT
	GTE      Condition = gripql.Condition_GTE
	LT       Condition = gripql.Condition_LT
	LTE      Condition = gripql.Condition_LTE
	INSIDE   Condition = gripql.Condition_INSIDE
	OUTSIDE  Condition = gripql.Condition_OUTSIDE
	BETWEEN  Condition = gripql.Condition_BETWEEN
	WITHIN   Condition = gripql.Condition_WITHIN
	WITHOUT  Condition = gripql.Condition_WITHOUT
	CONTAINS Condition = gripql.Condition_CONTAINS
)

type FieldFilter struct {
	Field    string
	Operator Condition
	Value    any
}
