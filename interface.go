package benchtop

import (
	"github.com/bmeg/benchtop/query"
)

type TableInfo struct {
	FileName string      `json:"fileName"`
	Columns  []ColumnDef `json:"columns"`
	TableId  uint16      `json:"tableid"`
	Path     string      `json:"path"`
	Name     string      `json:"name"`
}

type ColumnDef struct {
	Key string `json:"key"`
	// Type FieldType `json:"type"` Remove this for now since not using bson anymore
}

/*
	 Keep this code as a reminder for what the table field type architecture when bson was used
		 type FieldType bsontype.Type

		 const (
			Double      FieldType = FieldType(bson.TypeDouble)
			Int64       FieldType = FieldType(bson.TypeInt64)
			String      FieldType = FieldType(bson.TypeString)
			Bytes       FieldType = FieldType(bson.TypeBinary)
			VectorArray FieldType = FieldType(bson.TypeArray)
		 )
*/

type Row struct {
	Id        []byte
	TableName string
	Data      map[string]any
}

type Index struct {
	Key []byte
	Loc *RowLoc
}

type RowLocData struct {
	Data    []byte
	DataMap map[string]any
	Loc     *RowLoc
}

type RowLoc struct {
	TableId uint16
	Section uint16 // Sectioning allows for smaller Offset, Size
	Offset  uint32 // Max offset, size is 4GB
	Size    uint32 // Compressed size
	Index   uint16 // Index within the block
}

type RowFilter interface {
	Matches(row []byte, tableStr string) bool
	GetFilter() any
	IsNoOp() bool
	RequiredFields() []string
}

type TableDriver interface {
	New(name string, columns []ColumnDef) (TableStore, error)
	Get(name string) (TableStore, error)
	ListTableKeys(tableId uint16) (chan Index, error)
	GetAllColNames() chan string
	GetLabels(edges bool, removePrefix bool) chan string
	RowIdsByHas(field string, value any, op query.Condition) chan Index
	RowIdsByLabelFieldValue(label string, field string, value any, op query.Condition) chan Index
	List() []string
	Delete(name string) error
	Close()
	BulkLoad(name string, rows chan Row) error
	GetKV() any // Returns the underlying KV store (PebbleKV or BoltDB wrapper)
}

type TableStore interface {
	GetColumnDefs() []ColumnDef
	HasField(field string) bool
	AddRow(elem Row) (*RowLoc, error)
	AddRows(elems []Row) ([]*RowLoc, error)
	GetRow(loc *RowLoc) (map[string]any, error)
	GetRowLoc(id string) (*RowLoc, error)
	GetRows(locs []*RowLoc, section uint16) ([]map[string]any, []error)
	DeleteRow(loc *RowLoc, id []byte) error
	MarkDeleteTable(loc *RowLoc) error

	ScanDoc(filter RowFilter) chan map[string]any
	ScanDocProjected(fields []string, filter RowFilter) chan map[string]any
	ScanId(filter RowFilter) chan string
	ScanFull(filter RowFilter) chan RowLocData

	//Compact() error
	Close() error
}

// FieldInfo describes an indexed/searchable field for a label.
type FieldInfo struct {
	Label string
	Field string
}

// FieldDriver exposes field-index lifecycle operations that some callers
// (such as grip) require beyond the core TableDriver surface.
type FieldDriver interface {
	AddField(label, field string) error
	RemoveField(label, field string) error
	ListFields() []FieldInfo
	DeleteRowField(label, field, rowID string) error
	GetIDsForLabel(label string) chan string
}
