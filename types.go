package benchtop

import "fmt"

func CheckType(val any, t FieldType) (any, error) {
	switch t {
	case Int64:
		if x, ok := val.(int32); !ok {
			return int64(x), nil
		}
		if _, ok := val.(int64); !ok {
			return val, fmt.Errorf("not int64")
		}
	}
	return val, nil
}
