package filters

import (
	"github.com/bmeg/benchtop"
	"strings"
)

func PassesFilters(fieldValue any, filters []benchtop.FieldFilter) bool {
	for _, filter := range filters {
		if !applyFilterCondition(fieldValue, filter) {
			return false
		}
	}
	return true
}

func applyFilterCondition(fieldValue any, filter benchtop.FieldFilter) bool {
	switch v := fieldValue.(type) {
	case string:
		filterStr, ok := filter.Value.(string)
		if !ok {
			return false
		}
		return applyOperator(v, filter.Operator, filterStr)
	case int, int32, int64, float32, float64:
		return applyNumericOperator(v, filter.Operator, filter.Value)
	case bool:
		filterBool, ok := filter.Value.(bool)
		if !ok {
			return false
		}
		return applyBooleanOperator(v, filter.Operator, filterBool)
	default:
		return false
	}
}

func applyOperator(fieldValue string, operator benchtop.OperatorType, filterValue string) bool {
	switch operator {
	case benchtop.OP_EQ:
		return fieldValue == filterValue
	case benchtop.OP_NEQ:
		return fieldValue != filterValue
	case benchtop.OP_CONTAINS:
		return strings.Contains(fieldValue, filterValue)
	case benchtop.OP_STARTSWITH:
		return strings.HasPrefix(fieldValue, filterValue)
	case benchtop.OP_ENDSWITH:
		return strings.HasSuffix(fieldValue, filterValue)
	default:
		return false
	}
}

func applyNumericOperator(fieldValue any, operator benchtop.OperatorType, filterValue any) bool {
	// Convert the field value to a float for comparison purposes
	var fieldFloat float64
	switch v := fieldValue.(type) {
	case int:
		fieldFloat = float64(v)
	case int32:
		fieldFloat = float64(v)
	case int64:
		fieldFloat = float64(v)
	case float32:
		fieldFloat = float64(v)
	case float64:
		fieldFloat = v
	default:
		return false
	}

	// Convert filterValue to float
	var filterFloat float64
	switch v := filterValue.(type) {
	case int:
		filterFloat = float64(v)
	case int32:
		filterFloat = float64(v)
	case int64:
		filterFloat = float64(v)
	case float32:
		filterFloat = float64(v)
	case float64:
		filterFloat = v
	default:
		return false
	}

	// Compare using the operator
	switch operator {
	case benchtop.OpEqual:
		return fieldFloat == filterFloat
	case benchtop.OpNotEqual:
		return fieldFloat != filterFloat
	case benchtop.OpGreaterThan:
		return fieldFloat > filterFloat
	case benchtop.OpLessThan:
		return fieldFloat < filterFloat
	case benchtop.OpGreaterThanOrEqual:
		return fieldFloat >= filterFloat
	case benchtop.OpLessThanOrEqual:
		return fieldFloat <= filterFloat
	default:
		return false
	}
}

func applyBooleanOperator(fieldValue bool, operator benchtop.OperatorType, filterValue bool) bool {
	switch operator {
	case benchtop.OpEqual:
		return fieldValue == filterValue
	case benchtop.OpNotEqual:
		return fieldValue != filterValue
	default:
		return false
	}
}
