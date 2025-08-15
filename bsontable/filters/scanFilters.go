

package filters

import (

	"github.com/bmeg/benchtop"
	"strconv"
	"github.com/bytedance/sonic/ast"
	"github.com/bmeg/grip/log"
)



type FilterValue struct {
    StringVal  string
    IntVal     int64
    FloatVal   float64
    BoolVal    bool
    ArrayVal   []FilterValue
    ObjectVal  map[string]FilterValue
    IsString   bool
    IsInt      bool
    IsFloat    bool
    IsBool     bool
    IsNull     bool
    IsArray    bool
    IsObject   bool
}

// NewFilterValue precomputes a FilterValue from an interface{}
func NewFilterValue(val interface{}) FilterValue {
    fv := FilterValue{}
    switch v := val.(type) {
    case string:
        fv.StringVal = v
        fv.IsString = true
    case int:
        fv.IntVal = int64(v)
        fv.IsInt = true
    case int64:
        fv.IntVal = v
        fv.IsInt = true
    case float64:
        fv.FloatVal = v
        fv.IsFloat = true
    case float32:
        fv.FloatVal = float64(v)
        fv.IsFloat = true
    case bool:
        fv.BoolVal = v
        fv.IsBool = true
    case nil:
        fv.IsNull = true
    case []interface{}:
        fv.ArrayVal = make([]FilterValue, len(v))
        for i, elem := range v {
            fv.ArrayVal[i] = NewFilterValue(elem)
        }
        fv.IsArray = true
    case map[string]interface{}:
        fv.ObjectVal = make(map[string]FilterValue, len(v))
        for k, elem := range v {
            fv.ObjectVal[k] = NewFilterValue(elem)
        }
        fv.IsObject = true
    }
    return fv
}
func ApplyFilterCondition(node *ast.Node, cond *benchtop.FieldFilter) bool {
    // Early exit for null node with unsupported operators
    if node.TypeSafe() == ast.V_NULL && cond.Operator != benchtop.OP_EQ && cond.Operator != benchtop.OP_NEQ {
        return false
    }

    fv := cond.Value.(FilterValue)

    switch cond.Operator {
    case benchtop.OP_EQ:
        return nodeValueEqual(node, fv)

    case benchtop.OP_NEQ:
        return !nodeValueEqual(node, fv)

    case benchtop.OP_GT, benchtop.OP_GTE, benchtop.OP_LT, benchtop.OP_LTE:
        if !fv.IsFloat && !fv.IsInt {
            return false
        }
        valF, ok := nodeValueFloat64(node)
        if !ok {
            return false
        }
        condF := fv.FloatVal
        if fv.IsInt {
            condF = float64(fv.IntVal)
        }
        switch cond.Operator {
        case benchtop.OP_GT:
            return valF > condF
        case benchtop.OP_GTE:
            return valF >= condF
        case benchtop.OP_LT:
            return valF < condF
        case benchtop.OP_LTE:
            return valF <= condF
        }

    case benchtop.OP_INSIDE, benchtop.OP_OUTSIDE, benchtop.OP_BETWEEN:
        if !fv.IsArray || len(fv.ArrayVal) != 2 {
            return false
        }
        lower, ok := toFloat64(fv.ArrayVal[0])
        if !ok {
            return false
        }
        upper, ok := toFloat64(fv.ArrayVal[1])
        if !ok {
            return false
        }
        valF, ok := nodeValueFloat64(node)
        if !ok {
            return false
        }
        switch cond.Operator {
        case benchtop.OP_INSIDE:
            return valF > lower && valF < upper
        case benchtop.OP_OUTSIDE:
            return valF < lower || valF > upper
        case benchtop.OP_BETWEEN:
            return valF >= lower && valF <= upper
        }

    case benchtop.OP_WITHIN, benchtop.OP_WITHOUT:
        if !fv.IsArray {
            return cond.Operator == benchtop.OP_WITHOUT
        }
        found := nodeValueInSlice(node, fv.ArrayVal)
        if cond.Operator == benchtop.OP_WITHIN {
            return found
        }
        return !found

    case benchtop.OP_CONTAINS:
        if node.TypeSafe() != ast.V_ARRAY {
            return false
        }
        for i := 0; ; i++ {
            childNode := node.Index(i)
            if childNode == nil {
                break
            }
            if childNode.TypeSafe() == ast.V_NONE {
                continue
            }
            if nodeValueEqual(childNode, fv) {
                return true
            }
        }
        return false

    default:
        return false
    }
    return false
}

// nodeValueEqual compares an ast.Node value with a FilterValue
func nodeValueEqual(node *ast.Node, fv FilterValue) bool {
    switch node.TypeSafe() {
    case ast.V_STRING:
        if !fv.IsString {
            return false
        }
        s, err := node.String()
        return err == nil && s == fv.StringVal
    case ast.V_NUMBER:
        if fv.IsInt {
            n, err := node.Int64()
            return err == nil && n == fv.IntVal
        }
        if fv.IsFloat {
            n, err := node.Float64()
            return err == nil && n == fv.FloatVal
        }
        return false
    case ast.V_TRUE, ast.V_FALSE:
        if !fv.IsBool {
            return false
        }
        b, err := node.Bool()
        return err == nil && b == fv.BoolVal
    case ast.V_NULL:
        return fv.IsNull
    case ast.V_ARRAY:
        if !fv.IsArray {
            return false
        }
        arrLen, err := node.Len()
        if err != nil || arrLen != len(fv.ArrayVal) {
            return false
        }
        for i := 0; i < arrLen; i++ {
            childNode := node.Index(i)
            if childNode == nil || childNode.TypeSafe() == ast.V_NONE {
                return false
            }
            if !nodeValueEqual(childNode, fv.ArrayVal[i]) {
                return false
            }
        }
        return true
    case ast.V_OBJECT:
        if !fv.IsObject {
            return false
        }
        objLen, err := node.Len()
        if err != nil || objLen != len(fv.ObjectVal) {
            return false
        }
        for k, v := range fv.ObjectVal {
            childNode := node.Get(k)
            if childNode == nil || childNode.TypeSafe() == ast.V_NONE {
                return false
            }
            if !nodeValueEqual(childNode, v) {
                return false
            }
        }
        return true
    }
    return false
}

// nodeValueFloat64 reads a float64 from an ast.Node
func nodeValueFloat64(node *ast.Node) (float64, bool) {
    if node.TypeSafe() != ast.V_NUMBER {
        return 0, false
    }
    f, err := node.Float64()
    return f, err == nil
}

// nodeValueInSlice checks if an ast.Node value exists in a slice of FilterValues
func nodeValueInSlice(node *ast.Node, vals []FilterValue) bool {
    if len(vals) > 10 {
        // Build a set for faster lookup, but use nodeValueEqual to avoid Interface()
        for _, v := range vals {
            if nodeValueEqual(node, v) {
                return true
            }
        }
        return false
    }
    for _, v := range vals {
        if nodeValueEqual(node, v) {
            return true
        }
    }
    return false
}

// toFloat64 converts a FilterValue to float64
func toFloat64(fv FilterValue) (float64, bool) {
    if fv.IsInt {
        return float64(fv.IntVal), true
    }
    if fv.IsFloat {
        return fv.FloatVal, true
    }
    if fv.IsString {
        f, err := strconv.ParseFloat(fv.StringVal, 64)
        return f, err == nil
    }
    return 0, false
}

// ApplyBaseFilterCondition applies a filter to a raw value
func ApplyBaseFilterCondition(val any, cond *benchtop.FieldFilter) bool {
    fv, ok := cond.Value.(FilterValue)
    if !ok {
        log.Debugf("UserError: expected FilterValue, got %T", cond.Value)
        return false
    }

    if (val == nil || fv.IsNull) && cond.Operator != benchtop.OP_EQ && cond.Operator != benchtop.OP_NEQ && cond.Operator != benchtop.OP_WITHIN && cond.Operator != benchtop.OP_WITHOUT && cond.Operator != benchtop.OP_CONTAINS {
        return false
    }

    switch cond.Operator {
    case benchtop.OP_EQ:
        return filterValueEqual(val, fv)
    case benchtop.OP_NEQ:
        return !filterValueEqual(val, fv)
    case benchtop.OP_GT:
        valF, ok := toFloat64Any(val)
        if !ok {
            return false
        }
        condF, ok := toFloat64(fv)
        if !ok {
            return false
        }
        return valF > condF
    case benchtop.OP_GTE:
        valF, ok := toFloat64Any(val)
        if !ok {
            return false
        }
        condF, ok := toFloat64(fv)
        if !ok {
            return false
        }
        return valF >= condF
    case benchtop.OP_LT:
        valF, ok := toFloat64Any(val)
        if !ok {
            return false
        }
        condF, ok := toFloat64(fv)
        if !ok {
            return false
        }
        return valF < condF
    case benchtop.OP_LTE:
        valF, ok := toFloat64Any(val)
        if !ok {
            return false
        }
        condF, ok := toFloat64(fv)
        if !ok {
            return false
        }
        return valF <= condF
    case benchtop.OP_INSIDE:
        if !fv.IsArray || len(fv.ArrayVal) != 2 {
            log.Debugf("UserError: expected array of length 2 for INSIDE condition")
            return false
        }
        lower, ok := toFloat64(fv.ArrayVal[0])
        if !ok {
            return false
        }
        upper, ok := toFloat64(fv.ArrayVal[1])
        if !ok {
            return false
        }
        valF, ok := toFloat64Any(val)
        if !ok {
            return false
        }
        return valF > lower && valF < upper
    case benchtop.OP_OUTSIDE:
        if !fv.IsArray || len(fv.ArrayVal) != 2 {
            log.Debugf("UserError: expected array of length 2 for OUTSIDE condition")
            return false
        }
        lower, ok := toFloat64(fv.ArrayVal[0])
        if !ok {
            return false
        }
        upper, ok := toFloat64(fv.ArrayVal[1])
        if !ok {
            return false
        }
        valF, ok := toFloat64Any(val)
        if !ok {
            return false
        }
        return valF < lower || valF > upper
    case benchtop.OP_BETWEEN:
        if !fv.IsArray || len(fv.ArrayVal) != 2 {
            log.Debugf("UserError: expected array of length 2 for BETWEEN condition")
            return false
        }
        lower, ok := toFloat64(fv.ArrayVal[0])
        if !ok {
            return false
        }
        upper, ok := toFloat64(fv.ArrayVal[1])
        if !ok {
            return false
        }
        valF, ok := toFloat64Any(val)
        if !ok {
            return false
        }
        return valF >= lower && valF <= upper
    case benchtop.OP_WITHIN, benchtop.OP_WITHOUT:
        if !fv.IsArray {
            log.Debugf("UserError: expected array for %s condition, got %T", cond.Operator, fv)
            return cond.Operator == benchtop.OP_WITHOUT
        }
        found := false
        for _, v := range fv.ArrayVal {
            if filterValueEqual(val, v) {
                found = true
                break
            }
        }
        if cond.Operator == benchtop.OP_WITHIN {
            return found
        }
        return !found
    case benchtop.OP_CONTAINS:
        valSlice, ok := val.([]interface{})
        if !ok {
            log.Debugf("UserError: expected slice for CONTAINS condition, got %T", val)
            return false
        }
        for _, v := range valSlice {
            if filterValueEqual(v, fv) {
                return true
            }
        }
        return false
    default:
        log.Debugf("UserError: unknown operator %v", cond.Operator)
        return false
    }
}

// filterValueEqual compares a raw value with a FilterValue
func filterValueEqual(val any, fv FilterValue) bool {
    switch v := val.(type) {
    case string:
        return fv.IsString && v == fv.StringVal
    case int:
        return fv.IsInt && int64(v) == fv.IntVal
    case int64:
        return fv.IsInt && v == fv.IntVal
    case float64:
        return fv.IsFloat && v == fv.FloatVal
    case float32:
        return fv.IsFloat && float64(v) == fv.FloatVal
    case bool:
        return fv.IsBool && v == fv.BoolVal
    case nil:
        return fv.IsNull
    case []interface{}:
        if !fv.IsArray || len(v) != len(fv.ArrayVal) {
            return false
        }
        for i, elem := range v {
            if !filterValueEqual(elem, fv.ArrayVal[i]) {
                return false
            }
        }
        return true
    case map[string]interface{}:
        if !fv.IsObject || len(v) != len(fv.ObjectVal) {
            return false
        }
        for k, elem := range v {
            fvVal, ok := fv.ObjectVal[k]
            if !ok || !filterValueEqual(elem, fvVal) {
                return false
            }
        }
        return true
    default:
        return false
    }
}

func toFloat64Any(val any) (float64, bool) {
    switch v := val.(type) {
    case int:
        return float64(v), true
    case int64:
        return float64(v), true
    case float64:
        return v, true
    case float32:
        return float64(v), true
    case string:
        f, err := strconv.ParseFloat(v, 64)
        return f, err == nil
    default:
        return 0, false
    }
}