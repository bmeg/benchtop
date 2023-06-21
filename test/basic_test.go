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

	ts, err := benchtop.Create("test.data", []benchtop.ColumnDef{
		{Path: "field1", Type: benchtop.Double},
		{Path: "name", Type: benchtop.String},
	})

	if err != nil {
		t.Error(err)
	}

	offsets := map[string]int64{}

	for k, r := range data {
		o, err := ts.Add(r)
		if err != nil {
			t.Error(err)
		}
		offsets[k] = o
	}

	for k, o := range offsets {
		post, err := ts.Get(o)
		fmt.Printf("%#v\n", post)
		if err != nil {
			t.Error(err)
		}
		orig := data[k]
		for key := range orig {
			origVal := orig[key]
			postVal := post[key]
			if origVal != postVal {
				t.Errorf("offset: %d: %s != %s", o, origVal, postVal)
			}
		}
	}

	for k, o := range offsets {
		post, err := ts.Get(o, "name")
		fmt.Printf("%#v\n", post)
		if err != nil {
			t.Error(err)
		}
		orig := data[k]
		for key := range post {
			if key != "name" {
				t.Errorf("Get Select returned field %s", key)
			} else {
				origVal := orig[key]
				postVal := post[key]
				if origVal != postVal {
					t.Errorf("offset: %d: %s != %s", o, origVal, postVal)
				}
			}
		}
	}

	offsetList, err := ts.ListOffsets()
	if err != nil {
		t.Error(err)
	}
	oCount := 0
	for i := range offsetList {
		oCount++
		fmt.Printf("%d\n", i)
	}
	fmt.Printf("Offset count: %d\n", oCount)

	ts.Compact()

	ts.Close()
}
