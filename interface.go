package benchtop

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

type FieldFilter struct {
	Field    string
	Operator string // supported operators "==", "!=", ">", "<", ">=", "<=", "contains", "startswith", "endswith"
	Value    any
}

type TableInfo struct {
	Id      uint32      `json:"id"`
	Columns []ColumnDef `json:"columns"`
}

type ColumnDef struct {
	Name string    `json:"name"`
	Type FieldType `json:"type"`
}

type TableDriver interface {
	New(name string, columns []ColumnDef) (TableStore, error)
	Get(name string) (TableStore, error)
	List() []string
	Close()
}

type Entry struct {
	Key   []byte
	Value map[string]any
}

type Index struct {
	Key      []byte
	Position uint64
}

type TableStore interface {
	GetColumns() []ColumnDef
	Add(key []byte, row map[string]any) error
	Get(key []byte, fields ...string) (map[string]any, error)
	Delete(key []byte) error
	Fetch(inputs chan Index, workers int) <-chan struct {
		key  string
		data map[string]any
		err  string
	}

	Scan(filter []FieldFilter, fields ...string) (chan map[string]any, error)

	Keys() (chan Index, error)

	Load(chan Entry) error

	Compact() error
	Close()
}

type FieldType bsontype.Type

const (
	Double FieldType = FieldType(bson.TypeDouble)
	Int64  FieldType = FieldType(bson.TypeInt64)
	String FieldType = FieldType(bson.TypeString)
	Bytes  FieldType = FieldType(bson.TypeBinary)
)
