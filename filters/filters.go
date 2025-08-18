package filters

import (
	"reflect"

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
	if (val == nil || cond.Value == nil) &&
		cond.Operator != gripql.Condition_EQ &&
		cond.Operator != gripql.Condition_NEQ &&
		cond.Operator != gripql.Condition_WITHIN &&
		cond.Operator != gripql.Condition_WITHOUT &&
		cond.Operator != gripql.Condition_CONTAINS {
		return false
	}

	switch cond.Operator {
	case gripql.Condition_EQ:
		return reflect.DeepEqual(val, condVal)

	case gripql.Condition_NEQ:
		return !reflect.DeepEqual(val, condVal)

	case gripql.Condition_GT:
		valN, err := cast.ToFloat64E(val)
		if err != nil {
			return false
		}
		condN, err := cast.ToFloat64E(condVal)
		if err != nil {
			return false
		}
		return valN > condN

	case gripql.Condition_GTE:
		valN, err := cast.ToFloat64E(val)
		if err != nil {
			return false
		}
		condN, err := cast.ToFloat64E(condVal)
		if err != nil {
			return false
		}
		return valN >= condN

	case gripql.Condition_LT:
		//log.Debugf("match: %#v %#v %s", condVal, val, cond.Key)
		valN, err := cast.ToFloat64E(val)
		//log.Debugf("CAST: ", valN, "ERROR: ", err)
		if err != nil {
			return false
		}
		condN, err := cast.ToFloat64E(condVal)
		if err != nil {
			return false
		}
		return valN < condN

	case gripql.Condition_LTE:
		valN, err := cast.ToFloat64E(val)
		if err != nil {
			return false
		}
		condN, err := cast.ToFloat64E(condVal)
		if err != nil {
			return false
		}
		return valN <= condN

	case gripql.Condition_INSIDE:
		vals, err := cast.ToSliceE(condVal)
		if err != nil {
			log.Debugf("UserError: could not cast INSIDE condition value: %v", err)
			return false
		}
		if len(vals) != 2 {
			log.Debugf("UserError: expected slice of length 2 not %v for INSIDE condition value", len(vals))
			return false
		}
		lower, err := cast.ToFloat64E(vals[0])
		if err != nil {
			log.Debugf("UserError: could not cast lower INSIDE condition value: %v", err)
			return false
		}
		upper, err := cast.ToFloat64E(vals[1])
		if err != nil {
			log.Debugf("UserError: could not cast upper INSIDE condition value: %v", err)
			return false
		}
		valF, err := cast.ToFloat64E(val)
		if err != nil {
			log.Debugf("UserError: could not cast INSIDE value: %v", err)
			return false
		}
		return valF > lower && valF < upper

	case gripql.Condition_OUTSIDE:
		vals, err := cast.ToSliceE(condVal)
		if err != nil {
			log.Debugf("UserError: could not cast OUTSIDE condition value: %v", err)
			return false
		}
		if len(vals) != 2 {
			log.Debugf("UserError: expected slice of length 2 not %v for OUTSIDE condition value", len(vals))
			return false
		}
		lower, err := cast.ToFloat64E(vals[0])
		if err != nil {
			log.Debugf("UserError: could not cast lower OUTSIDE condition value: %v", err)
			return false
		}
		upper, err := cast.ToFloat64E(vals[1])
		if err != nil {
			log.Debugf("UserError: could not cast upper OUTSIDE condition value: %v", err)
			return false
		}
		valF, err := cast.ToFloat64E(val)
		if err != nil {
			log.Debugf("UserError: could not cast OUTSIDE value: %v", err)
			return false
		}
		return valF < lower || valF > upper

	case gripql.Condition_BETWEEN:
		vals, err := cast.ToSliceE(condVal)
		if err != nil {
			log.Debugf("UserError: could not cast BETWEEN condition value: %v", err)
			return false
		}
		if len(vals) != 2 {
			log.Debugf("UserError: expected slice of length 2 not %v for BETWEEN condition value", len(vals))
			return false
		}
		lower, err := cast.ToFloat64E(vals[0])
		if err != nil {
			log.Debugf("UserError: could not cast lower BETWEEN condition value: %v", err)
			return false
		}
		upper, err := cast.ToFloat64E(vals[1])
		if err != nil {
			log.Debugf("UserError: could not cast upper BETWEEN condition value: %v", err)
			return false
		}
		valF, err := cast.ToFloat64E(val)
		if err != nil {
			log.Debugf("UserError: could not cast BETWEEN value: %v", err)
			return false
		}
		return valF >= lower && valF < upper

	case gripql.Condition_WITHIN:
		found := false
		switch condVal := condVal.(type) {
		case []any:
			for _, v := range condVal {
				if reflect.DeepEqual(val, v) {
					found = true
				}
			}

		case nil:
			found = false

		default:
			log.Debugf("UserError: expected slice not %T for WITHIN condition value", condVal)
		}

		return found

	case gripql.Condition_WITHOUT:
		found := false
		switch condVal := condVal.(type) {
		case []any:
			for _, v := range condVal {
				if reflect.DeepEqual(val, v) {
					found = true
				}
			}

		case nil:
			found = false

		default:
			log.Debugf("UserError: expected slice not %T for WITHOUT condition value", condVal)

		}

		return !found

	case gripql.Condition_CONTAINS:
		found := false
		switch val := val.(type) {
		case []any:
			for _, v := range val {
				if reflect.DeepEqual(v, condVal) {
					found = true
				}
			}

		case nil:
			found = false

		default:
			log.Debugf("UserError: unknown condition value type %T for CONTAINS condition", val)
		}

		return found

	default:
		return false
	}
}
