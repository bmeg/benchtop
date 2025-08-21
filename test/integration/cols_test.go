package test

import (
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable"
	"github.com/bmeg/benchtop/util"
)

func TestGetAllColls(t *testing.T) {
	name := "test.data" + util.RandomString(5)
	defer os.RemoveAll(name)

	dr, err := jsontable.NewJSONDriver(name)
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
