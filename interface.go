package benchtop

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

type VectorSearchResult struct {
	Key      []byte
	Distance float32
	Vector   []float32
	Data     map[string]any
}

type FieldFilter struct {
	Field    string
	Operator string // supported operators "==", "!=", ">", "<", ">=", "<=", "contains", "startswith", "endswith"
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

type TableStore interface {
	GetColumnDefs() []ColumnDef
	AddRow(elem Row) error
	GetRow(key []byte, fields ...string) (map[string]any, error)
	DeleteRow(key []byte) error

	Fetch(inputs chan Index, workers int) <-chan BulkResponse
	Remove(inputs chan Index, workers int) <-chan BulkResponse
	Scan(key bool, filter []FieldFilter, fields ...string) (chan map[string]any, error)
	VectorSearch(string, []float32, int) ([]VectorSearchResult, error)

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
