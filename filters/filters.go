package filters

import (
	"errors"
	"reflect"
	"strconv"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/spf13/cast"
)

type FieldFilter struct {
	Field    string
	Operator gripql.Condition
	Value    any
}

func ApplyFilterCondition(val any, cond *FieldFilter) bool {
	condVal := cond.Value
	if (val == nil || condVal == nil) &&
		cond.Operator != gripql.Condition_EQ &&
		cond.Operator != gripql.Condition_NEQ &&
		cond.Operator != gripql.Condition_WITHIN &&
		cond.Operator != gripql.Condition_WITHOUT &&
		cond.Operator != gripql.Condition_CONTAINS {
		return false
	}

	switch cond.Operator {
	case gripql.Condition_EQ:
		switch v := val.(type) {
		case string:
			condS, ok := condVal.(string)
			return ok && v == condS
		case int:
			condI, ok := condVal.(int)
			return ok && v == condI
		case float64:
			condF, ok := condVal.(float64)
			return ok && v == condF
		case bool:
			condB, ok := condVal.(bool)
			return ok && v == condB
		case nil:
			return condVal == nil
		default:
			return reflect.DeepEqual(val, condVal)
		}

	case gripql.Condition_NEQ:
		switch v := val.(type) {
		case string:
			condS, ok := condVal.(string)
			return ok && v != condS
		case int:
			condI, ok := condVal.(int)
			return ok && v != condI
		case float64:
			condF, ok := condVal.(float64)
			return ok && v != condF
		case bool:
			condB, ok := condVal.(bool)
			return ok && v != condB
		case nil:
			return condVal != nil
		default:
			return !reflect.DeepEqual(val, condVal)
		}

	case gripql.Condition_GT, gripql.Condition_GTE, gripql.Condition_LT, gripql.Condition_LTE:
		valN, err := getFloat64(val) // Use optimized getter
		if err != nil {
			return false
		}
		condN, err := getFloat64(condVal) // Use optimized getter
		if err != nil {
			return false
		}

		if cond.Operator == gripql.Condition_GT {
			return valN > condN
		}
		if cond.Operator == gripql.Condition_GTE {
			return valN >= condN
		}
		if cond.Operator == gripql.Condition_LT {
			return valN < condN
		}
		if cond.Operator == gripql.Condition_LTE {
			return valN <= condN
		}
		return false // Should not be reached

	case gripql.Condition_INSIDE, gripql.Condition_OUTSIDE, gripql.Condition_BETWEEN:
		// Still requires slice check, but we can use the optimized getFloat64 inside
		vals, err := cast.ToSliceE(condVal)
		if err != nil || len(vals) != 2 {
			return false
		}

		lower, err := getFloat64(vals[0])
		if err != nil {
			return false
		}
		upper, err := getFloat64(vals[1])
		if err != nil {
			return false
		}
		valF, err := getFloat64(val)
		if err != nil {
			return false
		}

		if cond.Operator == gripql.Condition_INSIDE {
			return valF > lower && valF < upper
		}
		if cond.Operator == gripql.Condition_OUTSIDE {
			return valF < lower || valF > upper
		}
		if cond.Operator == gripql.Condition_BETWEEN {
			return valF >= lower && valF < upper
		}
		return false

	case gripql.Condition_WITHIN:
		// val is the single document value. condVal is the slice of allowed values.
		// Check if val is EQ to any element in condVal slice.
		condSlice, ok := condVal.([]any)
		if !ok {
			log.Debugf("UserError: expected slice not %T for WITHIN condition value", condVal)
			return false
		}
		for _, v := range condSlice {
			if ApplyFilterCondition(val, &FieldFilter{Operator: gripql.Condition_EQ, Value: v}) {
				return true // Found a match
			}
		}
		return false

	case gripql.Condition_WITHOUT:
		condSlice, ok := condVal.([]any)
		if !ok {
			log.Debugf("UserError: expected slice not %T for WITHIN condition value", condVal)
			return true
		}
		for _, v := range condSlice {
			if ApplyFilterCondition(val, &FieldFilter{Operator: gripql.Condition_EQ, Value: v}) {
				return false
			}
		}
		return true

	case gripql.Condition_CONTAINS:
		// val is the slice from the document. condVal is the single target element.
		// Check if any element in val slice is EQ to condVal.
		valSlice, ok := val.([]any)
		if !ok {
			log.Debugf("UserError: expected slice not %T for CONTAINS condition value", val)
			return false
		}
		for _, v := range valSlice {
			// Use the optimized EQ check recursively instead of reflect.DeepEqual(v, condVal)
			// Note: Arguments are v (slice element) and condVal (target).
			if ApplyFilterCondition(v, &FieldFilter{Operator: gripql.Condition_EQ, Value: condVal}) {
				return true // Found a match
			}
		}
		return false

	default:
		return false
	}
}

// getFloat64 is a highly optimized helper to convert 'any' value to float64,
// prioritizing direct type assertions (fastest) before falling back to strconv or cast (slower).
// This eliminates the repeated, slow calls to cast.ToFloat64E(val) for numeric comparisons.
func getFloat64(val any) (float64, error) {
	if val == nil {
		return 0, errors.New("cannot convert nil to float64")
	}
	switch v := val.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		// Use strconv for fast string-to-float conversion (more direct than cast)
		return strconv.ParseFloat(v, 64)
	default:
		// Fallback to cast for complex/unknown numeric types if necessary (e.g., json.Number)
		return cast.ToFloat64E(val)
	}
}
