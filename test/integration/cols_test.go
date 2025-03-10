package test

import (
	"os"
	"testing"

	"github.com/akrylysov/pogreb"
	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/benchtop/util"
)

func TestGetAllColls(t *testing.T) {
	name := "test.data" + util.RandomString(5)
	pogrebName := name + "pogreb"
	defer os.RemoveAll(name)
	defer os.RemoveAll(pogrebName)

	pg, err := pogreb.Open(pogrebName, nil)
	if err != nil {
		t.Error(err)
	}

	dr, err := bsontable.NewBSONDriver(name, pg)
	if err != nil {
		t.Error(err)
	}

	_, err = dr.New("table_1", []benchtop.ColumnDef{
		{Key: "field1", Type: benchtop.Double},
		{Key: "name1", Type: benchtop.String},
	})
	if err != nil {
		t.Error(err)
	}

	_, err = dr.New("table_2", []benchtop.ColumnDef{
		{Key: "field2", Type: benchtop.Double},
		{Key: "name2", Type: benchtop.String},
	})
	if err != nil {
		t.Error(err)
	}

	_, err = dr.New("table_3", []benchtop.ColumnDef{
		{Key: "field3", Type: benchtop.Double},
		{Key: "name3", Type: benchtop.String},
	})
	if err != nil {
		t.Error(err)
	}

	_, err = dr.New("table_4", []benchtop.ColumnDef{
		{Key: "field3", Type: benchtop.Double},
		{Key: "name3", Type: benchtop.String},
	})
	if err != nil {
		t.Error(err)
	}
	for col := range dr.GetAllColNames() {
		t.Log("COL: ", col)
	}

	defer dr.Close()
}
