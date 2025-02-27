package test

import (
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/benchtop/util"
)

func TestGetAllColls(t *testing.T) {
	name := "test.data" + util.RandomString(5)
	dr, err := bsontable.NewBSONDriver(name)
	if err != nil {
		t.Error(err)
	}

	_, err = dr.New("table_1", []benchtop.ColumnDef{
		{Name: "field1", Type: benchtop.Double},
		{Name: "name1", Type: benchtop.String},
	})
	if err != nil {
		t.Error(err)
	}

	_, err = dr.New("table_2", []benchtop.ColumnDef{
		{Name: "field2", Type: benchtop.Double},
		{Name: "name2", Type: benchtop.String},
	})
	if err != nil {
		t.Error(err)
	}

	_, err = dr.New("table_3", []benchtop.ColumnDef{
		{Name: "field3", Type: benchtop.Double},
		{Name: "name3", Type: benchtop.String},
	})
	if err != nil {
		t.Error(err)
	}

	_, err = dr.New("table_4", []benchtop.ColumnDef{
		{Name: "field3", Type: benchtop.Double},
		{Name: "name3", Type: benchtop.String},
	})
	if err != nil {
		t.Error(err)
	}
	for col := range dr.GetAllColNames() {
		t.Log("COL: ", col)
	}

	defer dr.Close()
	os.RemoveAll(name)
}
