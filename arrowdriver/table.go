package arrowdriver

import (
	"sync"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/bmeg/benchtop"
	"go.etcd.io/bbolt"
)

const (
	arrowFileExt = ".arrow"
	indexFileExt = ".idx"
	idColumn     = "_id"
	dataColumn   = "_data"

	idsBucket      = "ids"
	metaBucket     = "meta"
	metaTableIDKey = "table_id"
	metaNextSecKey = "next_section"
	metaColumnsKey = "columns"
	metaIndexKey   = "indexed_fields"

	fieldIndexBucket        = "field_index"
	reverseFieldIndexBucket = "reverse_field_index"
)

const sectionWriteBatchRows = 16384

const defaultCompactRowsPerSection = 50000

type columnEncoding uint8

const (
	encString columnEncoding = iota
	encFloat64
	encBool
	encJSON
)

type ArrowTable struct {
	name    string
	baseDir string
	tableID uint16
	columns []benchtop.ColumnDef
	schema  *arrow.Schema

	indexedFields map[string]struct{}
	writeHintKeys []string
	writeHintEnc  map[string]columnEncoding
	writeHintOnly bool

	indexPath string
	indexDB   *bbolt.DB
	lock      sync.RWMutex

	columnCacheLock  sync.RWMutex
	columnCache      map[string][]any
	columnCacheOrder []string
	columnCacheCap   int

	sectionRowCacheLock  sync.RWMutex
	sectionRowCache      map[uint16][]map[string]any
	sectionRowCacheOrder []uint16
	sectionRowCacheCap   int

	valueIndexCacheLock sync.RWMutex
	valueIndexCache     map[string]map[string][]indexedRow

	rowOrdinalCacheLock sync.RWMutex
	rowOrdinalRows      []indexedRow
	rowOrdinalByID      map[string]uint32
}

type indexedRow struct {
	id  string
	loc *benchtop.RowLoc
}
