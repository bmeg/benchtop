package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/benchtop/test/fixtures"
	"github.com/bmeg/benchtop/util"
)

func TestOpenClose(t *testing.T) {
	name := "test.data" + util.RandomString(5)
	dr, err := bsontable.NewBSONDriver(name)
	if err != nil {
		t.Error(err)
	}

	_, err = dr.New("table_1", []benchtop.ColumnDef{
		{Name: "field1", Type: benchtop.Double},
		{Name: "name", Type: benchtop.String},
	})

	if err != nil {
		t.Error(err)
	}
	dr.Close()

	or, err := bsontable.NewBSONDriver(name)
	if err != nil {
		t.Error(err)
	}
	ot, err := or.Get("table_1")
	if err != nil {
		t.Error(err)
	}

	t.Log("COLS: ", ot.GetColumns())
	if len(ot.GetColumns()) != 2 {
		t.Errorf("Incorrect re-open")
	}
	defer or.Close()
	os.RemoveAll(name)
}

func TestInsert(t *testing.T) {
	dbname := "test.data" + util.RandomString(5)

	dr, err := bsontable.NewBSONDriver(dbname)
	if err != nil {
		t.Error(err)
	}

	ts, err := dr.New("table_1", []benchtop.ColumnDef{
		{Name: "field1", Type: benchtop.Double},
		{Name: "name", Type: benchtop.String},
	})

	if err != nil {
		t.Error(err)
	}

	for k, r := range fixtures.Basicdata {
		err := ts.Add([]byte(k), r)
		if err != nil {
			t.Error(err)
		}
	}

	for k := range fixtures.Basicdata {
		post, err := ts.Get([]byte(k))
		fmt.Printf("%#v\n", post)
		if err != nil {
			t.Error(err)
		}
		orig := fixtures.Basicdata[k]
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
		if _, ok := fixtures.Basicdata[string(i.Key)]; !ok {
			t.Errorf("Unknown key returned: %s", string(i.Key))
		}
		fmt.Printf("%s\n", string(i.Key))
	}
	if oCount != len(fixtures.Basicdata) {
		t.Errorf("Incorrect key count %d != %d", oCount, len(fixtures.Basicdata))
	}

	ts.Compact()
	defer dr.Close()
	os.RemoveAll(dbname)
}
