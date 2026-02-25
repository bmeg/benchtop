package query

// Condition is benchtop's internal filter operator enum.
// Values are kept aligned with GripQL condition numeric values for compatibility.
type Condition int32

const (
	UNKNOWN_CONDITION Condition = 0
	EQ                Condition = 1
	NEQ               Condition = 2
	GT                Condition = 3
	GTE               Condition = 4
	LT                Condition = 5
	LTE               Condition = 6
	INSIDE            Condition = 7
	OUTSIDE           Condition = 8
	BETWEEN           Condition = 9
	WITHIN            Condition = 10
	WITHOUT           Condition = 11
	CONTAINS          Condition = 12
)

type FieldFilter struct {
	Field    string
	Operator Condition
	Value    any
}
