package test

import (
	"fmt"
	"testing"

	"github.com/bmeg/benchtop"
)

var data = map[string]map[string]any{
	"key1": {
		"field1": 0.1,
		"name":   "alice",
		"other":  "other data",
	},
	"key2": {
		"field1": 0.2,
		"name":   "bob",
	},
	"key3": {
		"field1": 0.3,
		"name":   "chelsie",
	},
}

func TestInsert(t *testing.T) {

	dr, err := benchtop.NewBSONDriver("test.data")
	if err != nil {
		t.Error(err)
	}

	ts, err := dr.New("table_1", []benchtop.ColumnDef{
		{Path: "field1", Type: benchtop.Double},
		{Path: "name", Type: benchtop.String},
	})

	if err != nil {
		t.Error(err)
	}

	for k, r := range data {
		err := ts.Add([]byte(k), r)
		if err != nil {
			t.Error(err)
		}
	}

	for k := range data {
		post, err := ts.Get([]byte(k))
		fmt.Printf("%#v\n", post)
		if err != nil {
			t.Error(err)
		}
		orig := data[k]
		for key := range orig {
			origVal := orig[key]
			postVal := post[key]
			if origVal != postVal {
				t.Errorf("offset: %s: %s != %s", k, origVal, postVal)
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
		fmt.Printf("%s\n", i)
	}
	fmt.Printf("Offset count: %d\n", oCount)

	ts.Compact()

	ts.Close()
}
