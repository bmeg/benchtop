package benchtop

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

type TableStore interface {
	Close()
	GetColumns() []ColumnDef
	Add(row map[string]any) (int64, error)
	Get(offset int64, fields ...string) (map[string]any, error)
	ListOffsets() (chan int64, error)

	OffsetToPosition(offset int64) (int64, error)

	Compact() error
	Delete(offset int64) error
}

type FieldType bsontype.Type

const (
	Double FieldType = FieldType(bson.TypeDouble)
	Int64  FieldType = FieldType(bson.TypeInt64)
	String FieldType = FieldType(bson.TypeString)
)

type ColumnDef struct {
	Path string    `json:"path"`
	Type FieldType `json:"type"`
}
