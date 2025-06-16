package filters

import (
	"reflect"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/grip/log"
	"github.com/spf13/cast"
)

func ApplyFilterCondition(val any, cond *benchtop.FieldFilter) bool {
	condVal := cond.Value
	if (val == nil || cond.Value == nil) &&
		cond.Operator != benchtop.OP_EQ &&
		cond.Operator != benchtop.OP_NEQ &&
		cond.Operator != benchtop.OP_WITHIN &&
		cond.Operator != benchtop.OP_WITHOUT &&
		cond.Operator != benchtop.OP_CONTAINS {
		return false
	}

	switch cond.Operator {
	case benchtop.OP_EQ:
		return reflect.DeepEqual(val, condVal)

	case benchtop.OP_NEQ:
		return !reflect.DeepEqual(val, condVal)

	case benchtop.OP_GT:
		valN, err := cast.ToFloat64E(val)
		if err != nil {
			return false
		}
		condN, err := cast.ToFloat64E(condVal)
		if err != nil {
			return false
		}
		return valN > condN

	case benchtop.OP_GTE:
		valN, err := cast.ToFloat64E(val)
		if err != nil {
			return false
		}
		condN, err := cast.ToFloat64E(condVal)
		if err != nil {
			return false
		}
		return valN >= condN

	case benchtop.OP_LT:
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

	case benchtop.OP_LTE:
		valN, err := cast.ToFloat64E(val)
		if err != nil {
			return false
		}
		condN, err := cast.ToFloat64E(condVal)
		if err != nil {
			return false
		}
		return valN <= condN

	case benchtop.OP_INSIDE:
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

	case benchtop.OP_OUTSIDE:
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

	case benchtop.OP_BETWEEN:
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

	case benchtop.OP_WITHIN:
		found := false
		switch condVal := condVal.(type) {
		case []interface{}:
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

	case benchtop.OP_WITHOUT:
		found := false
		switch condVal := condVal.(type) {
		case []interface{}:
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

	case benchtop.OP_CONTAINS:
		found := false
		switch val := val.(type) {
		case []interface{}:
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
