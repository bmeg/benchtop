package table

import (
	"fmt"

	"github.com/bmeg/benchtop"
	"github.com/cockroachdb/pebble"
)

type RowData struct {
	Data map[string]any `json:"0"`
	Key  string         `json:"1"`
}

func (b *JSONTable) PackData(entry map[string]any, key string) *RowData {
	return &RowData{
		Data: entry,
		Key:  key,
	}
}

func (b *JSONTable) GetTableEntryInfo(snap *pebble.Snapshot, id []byte) (*benchtop.RowLoc, error) {
	if b == nil {
		return nil, fmt.Errorf("JSONTable is nil")
	}
	if snap == nil {
		return nil, fmt.Errorf("snapshot is nil")
	}
	// Really only want to see if anything was returned or not. Since this doesn't interact
	// with the pebble indices, keep it in JSONTable
	_, closer, err := snap.Get(benchtop.NewPosKey(b.TableId, id))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return &benchtop.RowLoc{}, nil
}
func TableLabel(tableName string) string {
	if len(tableName) > 2 && (tableName[:2] == "v_" || tableName[:2] == "e_") {
		return tableName[2:]
	}
	return tableName
}
