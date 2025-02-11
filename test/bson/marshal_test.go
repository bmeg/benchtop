package test

import (
	"testing"

	"github.com/bmeg/benchtop"
	"go.mongodb.org/mongo-driver/bson"
)

func TestMarshal(t *testing.T) {

	tinfo := benchtop.TableInfo{
		Columns: []benchtop.ColumnDef{
			{Path: "columnA", Type: benchtop.String},
		},
		Id: 42,
	}

	md, err := bson.Marshal(tinfo)
	if err != nil {
		t.Errorf("error: %s", err)
	}

	out := benchtop.TableInfo{}

	err = bson.Unmarshal(md, &out)
	if err != nil {
		t.Errorf("error: %s", err)
	}

	if len(tinfo.Columns) != len(out.Columns) {
		t.Errorf("invalid unmarshal")
	}

	for i := range tinfo.Columns {
		if tinfo.Columns[i].Path != out.Columns[i].Path {
			t.Errorf("invalid unmarshal")
		}
		if tinfo.Columns[i].Type != out.Columns[i].Type {
			t.Errorf("invalid unmarshal")
		}
	}
}
