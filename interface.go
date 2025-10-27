package benchtop

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
	Loc RowLoc
}

type RowLoc struct {
	TableId uint16
	Section uint16 // Sectioning allows for smaller Offset, Size
	Offset  uint32 // Max offset, size is 4GB
	Size    uint32
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
	List() []string
	Delete(name string) error
	Close()
}

type TableStore interface {
	GetColumnDefs() []ColumnDef
	AddRow(elem Row) (*RowLoc, error)
	GetRow(loc *RowLoc) (map[string]any, error)
	DeleteRow(loc *RowLoc, id []byte) error

	ScanDoc(filter RowFilter) chan map[string]any
	ScanId(filter RowFilter) chan string

	//Compact() error
	Close() error
}
