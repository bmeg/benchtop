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

type TableDriver interface {
	New(name string, columns []ColumnDef) (TableStore, error)
	Get(name string) (TableStore, error)
	GetAllColNames() chan string
	GetLabels(edges bool, removePrefix bool) chan string
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

type RowLoc struct {
	Offset uint64
	Size   uint64
	Label  uint16
}

type RowFilter interface {
	Matches(row any) bool
	GetFilter() any
	IsNoOp() bool
	RequiredFields() []string
}

type TableStore interface {
	GetColumnDefs() []ColumnDef
	AddRow(elem Row) (*RowLoc, error)
	GetRow(loc RowLoc) (map[string]any, error)
	DeleteRow(key []byte) error

	Fetch(inputs chan Index, workers int) <-chan BulkResponse
	Remove(inputs chan Index, workers int) <-chan BulkResponse
	Scan(key bool, filter RowFilter) chan any
	Load(chan Row) error
	Keys() (chan Index, error)

	Compact() error
	Close()
}
