package filters

import (
	"errors"
	"reflect"
	"strconv"

	"github.com/bmeg/benchtop/query"
	"github.com/spf13/cast"
)

type FieldFilter = query.FieldFilter

func ApplyFilterCondition(val any, cond *FieldFilter) bool {
	condVal := cond.Value
	if (val == nil || condVal == nil) &&
		cond.Operator != query.EQ &&
		cond.Operator != query.NEQ &&
		cond.Operator != query.WITHIN &&
		cond.Operator != query.WITHOUT &&
		cond.Operator != query.CONTAINS {
		return false
	}

	switch cond.Operator {
	case query.EQ:
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

	case query.NEQ:
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

	case query.GT, query.GTE, query.LT, query.LTE:
		valN, err := getFloat64(val) // Use optimized getter
		if err != nil {
			return false
		}
		condN, err := getFloat64(condVal) // Use optimized getter
		if err != nil {
			return false
		}

		if cond.Operator == query.GT {
			return valN > condN
		}
		if cond.Operator == query.GTE {
			return valN >= condN
		}
		if cond.Operator == query.LT {
			return valN < condN
		}
		if cond.Operator == query.LTE {
			return valN <= condN
		}
		return false // Should not be reached

	case query.INSIDE, query.OUTSIDE, query.BETWEEN:
		// Still requires slice check, but we can use the optimized getFloat64 inside
		vals, err := cast.ToSliceE(condVal)
		if err != nil || len(vals) != 2 {
			return false
		}
		valN, err := getFloat64(val)
		if err != nil {
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

		if cond.Operator == query.INSIDE {
			return valN > lower && valN < upper
		}
		if cond.Operator == query.BETWEEN {
			return valN >= lower && valN <= upper
		}
		if cond.Operator == query.OUTSIDE {
			return valN < lower || valN > upper
		}
		return false

	case query.WITHIN:
		vals, err := cast.ToSliceE(condVal)
		if err != nil {
			return false
		}
		for _, v := range vals {
			if reflect.DeepEqual(val, v) {
				return true
			}
		}
		return false

	case query.WITHOUT:
		vals, err := cast.ToSliceE(condVal)
		if err != nil {
			return false
		}
		for _, v := range vals {
			if reflect.DeepEqual(val, v) {
				return false
			}
		}
		return true

	case query.CONTAINS:
		vals, err := cast.ToSliceE(val)
		if err != nil {
			return false
		}
		for _, v := range vals {
			if reflect.DeepEqual(v, condVal) {
				return true
			}
		}
		return false
	}

	return false
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
