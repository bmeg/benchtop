package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/akrylysov/pogreb"
	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/benchtop/util"
)

var data = map[string]map[string]any{
	"key1": {
		"field1": 0.1,
		"other":  "other data",
	},
	"key2": {
		"field1": 0.2,
		"other":  "other data",
	},
	"key3": {
		"field1": 0.3,
		"other":  "other data",
	},
}

func TestOpenClose(t *testing.T) {
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
		{Key: "other", Type: benchtop.String},
	})

	if err != nil {
		t.Error(err)
	}
	dr.Close()

	or, err := bsontable.NewBSONDriver(name, pg)
	if err != nil {
		t.Error(err)
	}
	ot, err := or.Get("table_1")
	if err != nil {
		t.Error(err)
	}

	if len(ot.GetColumnDefs()) != 2 {
		t.Errorf("Incorrect re-open")
	}
	defer or.Close()
}

func TestInsert(t *testing.T) {
	dbname := "test.data" + util.RandomString(5)
	pogrebName := dbname + "pogreb"
	defer os.RemoveAll(dbname)
	defer os.RemoveAll(pogrebName)

	pg, err := pogreb.Open(pogrebName, nil)
	if err != nil {
		t.Error(err)
	}

	dr, err := bsontable.NewBSONDriver(dbname, pg)
	if err != nil {
		t.Error(err)
	}

	ts, err := dr.New("table_1", []benchtop.ColumnDef{
		{Key: "field1", Type: benchtop.Double},
		{Key: "other", Type: benchtop.String},
	})

	if err != nil {
		t.Error(err)
	}

	for k, r := range data {
		err := ts.AddRow(benchtop.Row{Id: []byte(k), Data: r})
		if err != nil {
			t.Error(err)
		}
	}

	for k := range data {
		post, err := ts.GetRow([]byte(k))
		fmt.Printf("%#v\n", post)
		if err != nil {
			t.Error(err)
		}
		orig := data[k]
		for key := range orig {
			origVal := orig[key]
			postVal := post[key]
			if origVal != postVal {
				t.Errorf("key value: %s: %s != %s", k, origVal, postVal)
			}
		}
	}
	keyList, err := ts.Keys()
	if err != nil {
		t.Error(err)
	}
	oCount := 0
	for i := range keyList {
		oCount++
		if _, ok := data[string(i.Key)]; !ok {
			t.Errorf("Unknown key returned: %s", string(i.Key))
		}
		fmt.Printf("%s\n", string(i.Key))
	}
	if oCount != len(data) {
		t.Errorf("Incorrect key count %d != %d", oCount, len(data))
	}

	ts.Compact()
	defer dr.Close()
}

func TestDeleteTable(t *testing.T) {
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
		{Key: "other", Type: benchtop.String},
	})
	if err != nil {
		t.Error(err)
	}

	err = dr.Delete("table_1")
	if err != nil {
		t.Error(err)
	}

	dr.Close()

	or, err := bsontable.NewBSONDriver(name, pg)
	if err != nil {
		t.Error(err)
	}

	_, err = or.Get("table_1")
	if err == nil {
		t.Errorf("expected table to be gone. table still exists")
	}

	defer or.Close()
}
