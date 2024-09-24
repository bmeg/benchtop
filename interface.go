package benchtop

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

type FieldFilter struct {
	Field string
	Value string
}

type TableInfo struct {
	Id      uint32      `json:"id"`
	Columns []ColumnDef `json:"columns"`
}

type ColumnDef struct {
	Path string    `json:"path"`
	Type FieldType `json:"type"`
}

type TableDriver interface {
	New(name string, columns []ColumnDef) (TableStore, error)
	Get(name string) (TableStore, error)
	Close()
}

type TableStore interface {
	GetColumns() []ColumnDef
	Add(key []byte, row map[string]any) error
	Get(key []byte, fields ...string) (map[string]any, error)
	Delete(key []byte) error

	Scan(filter []FieldFilter, fields ...string) chan map[string]any

	Keys() (chan []byte, error)

	Compact() error
	Close()
}

type FieldType bsontype.Type

const (
	Double FieldType = FieldType(bson.TypeDouble)
	Int64  FieldType = FieldType(bson.TypeInt64)
	String FieldType = FieldType(bson.TypeString)
)
