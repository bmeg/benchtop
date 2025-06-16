package benchtop

import (
	"github.com/bmeg/benchtop/pebblebulk"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

type OperatorType string

const (
	OP_EQ         OperatorType = "=="
	OP_NEQ        OperatorType = "!="
	OP_GT         OperatorType = ">"
	OP_LT         OperatorType = "<"
	OP_GTE        OperatorType = ">="
	OP_LTE        OperatorType = "<="
	OP_INSIDE     OperatorType = "INSIDE"
	OP_OUTSIDE    OperatorType = "OUTSIDE"
	OP_BETWEEN    OperatorType = "BETWEEN"
	OP_WITHIN     OperatorType = "WITHIN"
	OP_WITHOUT    OperatorType = "WITHOUT"
	OP_CONTAINS   OperatorType = "CONTAINS"
	OP_STARTSWITH OperatorType = "STARTSWITH"
	OP_ENDSWITH   OperatorType = "ENDSWITH"
)

type FieldFilter struct {
	Field    string
	Operator OperatorType
	Value    any
}

type TableInfo struct {
	Id       uint32      `json:"id"`
	FileName string      `json:"fileName"`
	Columns  []ColumnDef `json:"columns"`
}

type ColumnDef struct {
	Key  string    `json:"key"`
	Type FieldType `json:"type"`
}

type TableDriver interface {
	New(name string, columns []ColumnDef) (TableStore, error)
	Get(name string) (TableStore, error)
	GetAllColNames() chan string
	GetLabels(edges bool) chan string
	List() []string
	Delete(name string) error
	Close()
}

type Row struct {
	Id        []byte
	TableName string
	Data      map[string]any
}

type Index struct {
	Key      []byte
	Position uint64
	Size     uint64
}

type BulkResponse struct {
	Key  []byte
	Data map[string]any
	Err  string
}

type RowFilter interface {
	// Matches returns true if the row passes the filter.
	Matches(row map[string]any) bool

	// RequiredFields returns a slice of field names needed to evaluate the filter.
	RequiredFields() []string
}


type TableStore interface {
	GetColumnDefs() []ColumnDef
	AddRow(elem Row, tx *pebblebulk.PebbleBulk) error
	GetRow(key []byte, fields ...string) (map[string]any, error)
	DeleteRow(key []byte) error

	Fetch(inputs chan Index, workers int) <-chan BulkResponse
	Remove(inputs chan Index, workers int) <-chan BulkResponse
	Scan(key bool, filter RowFilter, fields ...string) chan any
	Load(chan Row) error
	Keys() (chan Index, error)

	Compact() error
	Close()
}

type FieldType bsontype.Type

const (
	Double      FieldType = FieldType(bson.TypeDouble)
	Int64       FieldType = FieldType(bson.TypeInt64)
	String      FieldType = FieldType(bson.TypeString)
	Bytes       FieldType = FieldType(bson.TypeBinary)
	VectorArray FieldType = FieldType(bson.TypeArray)
)
