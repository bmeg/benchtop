package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable"
	jTable "github.com/bmeg/benchtop/jsontable/table"

	"github.com/bmeg/benchtop/util"
	"github.com/bmeg/grip/log"
	"github.com/cockroachdb/pebble"
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
	defer os.RemoveAll(name)

	dr, err := jsontable.NewJSONDriver(name)
	if err != nil {
		t.Error(err)
	}

	_, err = dr.New("table_1", []benchtop.ColumnDef{
		{Key: "field1"},
		{Key: "other"},
	})

	if err != nil {
		t.Error(err)
	}
	dr.Close()

	or, err := jsontable.NewJSONDriver(name)
	if err != nil {
		t.Error(err)
	}
	tid, err := or.LookupTableID("table_1")
	if err != nil {
		t.Error(err)
	}
	ot, err := or.Get(tid)
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
	defer os.RemoveAll(dbname)

	dr, err := jsontable.NewJSONDriver(dbname)
	if err != nil {
		t.Error(err)
	}
	ts, err := dr.New("table_1", []benchtop.ColumnDef{
		{Key: "field1"},
		{Key: "other"},
	})
	if err != nil {
		t.Error(err)
	}

	jT, _ := ts.(*jTable.JSONTable)
	// jDR, _ := dr.(*jsontable.JSONDriver) // Unused?

	jDR, _ := dr.(*jsontable.JSONDriver)

	for k, r := range data {
		loc, err := jT.AddRow(benchtop.Row{Id: []byte(k), TableID: jT.TableId, Data: r})
		if err != nil {
			t.Error(err)
		}

		pKey := benchtop.NewPosKey(jT.TableId, []byte(k))
		err = jDR.Pkv.Db.Set(pKey, benchtop.EncodeRowLoc(loc), pebble.Sync)
		if err != nil {
			t.Error(err)
		}
	}
	// for k := range data { ... }

	for k := range data {
		pKey := benchtop.NewPosKey(jT.TableId, []byte(k))
		val, closer, err := jDR.Pkv.Db.Get(pKey)
		if err != nil {
			if err != pebble.ErrNotFound {
				log.Errorf("Err on dr.Pb.Get for key %s in CacheLoader: %v", k, err)
			}
			log.Errorln("ERR: ", err)
			t.Fatal(err)
		}
		loc := benchtop.DecodeRowLoc(val)
		if closer != nil {
			closer.Close()
		}

		post, err := ts.GetRow(loc)
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
	// ListTableKeys is on JSONDriver but not in interface?
	// It IS in JSONDriver struct methods.
	keyList, err := jDR.ListTableKeys(jT.TableId)
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
	defer dr.Close()
}

func TestDeleteTable(t *testing.T) {
	name := "test.data" + util.RandomString(5)
	defer os.RemoveAll(name)

	dr, err := jsontable.NewJSONDriver(name)
	if err != nil {
		t.Error(err)
	}

	_, err = dr.New("table_1", []benchtop.ColumnDef{
		{Key: "field1"},
		{Key: "other"},
	})
	if err != nil {
		t.Error(err)
	}

	tid, _ := dr.LookupTableID("table_1")
	err = dr.Delete(tid)
	if err != nil {
		t.Error(err)
	}

	dr.Close()

	or, err := jsontable.NewJSONDriver(name)
	if err != nil {
		t.Error(err)
	}

	tid2, err := or.LookupTableID("table_1")
	if err == nil {
		_, err = or.Get(tid2)
		// If Lookup succeeded, Get might succeed.
		// But Delete should remove it from mapping?
		// If Delete works, LookupTableID might fail or return error.
		// Let's check Lookup error.
	}
	if err == nil {
		t.Errorf("expected table to be gone. table still exists")
	}

	defer or.Close()
}
