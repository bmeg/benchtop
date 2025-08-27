package test

import (
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bytedance/sonic"
)

func TestMarshal(t *testing.T) {

	tinfo := benchtop.TableInfo{
		Columns: []benchtop.ColumnDef{
			{Key: "columnA"},
		},
		TableId: 42,
	}

	md, err := sonic.ConfigFastest.Marshal(tinfo)
	if err != nil {
		t.Errorf("error: %s", err)
	}

	out := benchtop.TableInfo{}

	err = sonic.ConfigFastest.Unmarshal(md, &out)
	if err != nil {
		t.Errorf("error: %s", err)
	}

	if len(tinfo.Columns) != len(out.Columns) {
		t.Errorf("invalid unmarshal")
	}

	for i := range tinfo.Columns {
		if tinfo.Columns[i].Key != out.Columns[i].Key {
			t.Errorf("invalid unmarshal")
		}
	}
}
