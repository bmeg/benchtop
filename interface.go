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

type Row struct {
	Id      []byte
	TableID uint16
	Data    map[string]any
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
	Get(tableID uint16) (TableStore, error)
	Delete(tableID uint16) error
	Close()
	InvalidateLoc(tableId uint16, rowId string)
	BulkLoad(tableID uint16, rows chan *Row) error

	// Discovery and Metadata
	LookupTableID(name string) (uint16, error)
	ListTableIDs() []uint16
	GetTableInfo(tableID uint16) (*TableInfo, error)

	GetAllColNames() chan string
	GetLabels(edges bool, removePrefix bool) chan string
	RowIdsByHas(field string, value any, op query.Condition) chan Index
	RowIdsByTableFieldValue(tableID uint16, field string, value any, op query.Condition) chan Index
	List() []string
	GetKV() any // Returns the underlying KV store
}

type TableStore interface {
	GetColumnDefs() []ColumnDef
	HasField(field string) bool
	AddRow(elem Row) (*RowLoc, error)
	AddRows(elems []Row) ([]*RowLoc, error)
	GetRow(loc *RowLoc) (map[string]any, error)
	GetRowLoc(id string) (*RowLoc, error)
	GetRows(locs []*RowLoc) ([]map[string]any, []error)
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

// FieldDriver exposes field-index lifecycle operations.
type FieldDriver interface {
	AddField(tableID uint16, field string) error
	RemoveField(tableID uint16, field string) error
	ListFields() []FieldInfo
	DeleteRowField(tableID uint16, field, rowID string) error
	GetIDsForTable(tableID uint16) chan string
}
