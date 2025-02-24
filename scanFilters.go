package benchtop

import (
	"strings"
)

func PassesFilters(fieldValue any, filters []FieldFilter) bool {
	for _, filter := range filters {
		if !applyFilterCondition(fieldValue, filter) {
			return false
		}
	}
	return true
}

func applyFilterCondition(fieldValue any, filter FieldFilter) bool {
	switch v := fieldValue.(type) {
	case string:
		filterStr, ok := filter.Value.(string)
		if !ok {
			return false
		}
		return applyStringOperator(v, filter.Operator, filterStr)
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

func applyStringOperator(fieldValue string, operator string, filterValue string) bool {
	switch operator {
	case "==":
		return fieldValue == filterValue
	case "!=":
		return fieldValue != filterValue
	case "contains":
		return strings.Contains(fieldValue, filterValue)
	case "startswith":
		return strings.HasPrefix(fieldValue, filterValue)
	case "endswith":
		return strings.HasSuffix(fieldValue, filterValue)
	default:
		return false
	}
}

func applyNumericOperator(fieldValue any, operator string, filterValue any) bool {
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
	case "==":
		return fieldFloat == filterFloat
	case "!=":
		return fieldFloat != filterFloat
	case ">":
		return fieldFloat > filterFloat
	case "<":
		return fieldFloat < filterFloat
	case ">=":
		return fieldFloat >= filterFloat
	case "<=":
		return fieldFloat <= filterFloat
	default:
		return false
	}
}

func applyBooleanOperator(fieldValue bool, operator string, filterValue bool) bool {
	switch operator {
	case "==":
		return fieldValue == filterValue
	case "!=":
		return fieldValue != filterValue
	default:
		return false
	}
}
