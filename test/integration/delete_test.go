package test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable"
	jTable "github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/benchtop/util"
)

func TestDelete(t *testing.T) {
	dbname := "test.data" + util.RandomString(5)
	defer os.RemoveAll(dbname)

	dr, err := jsontable.NewJSONDriver(dbname)
	if err != nil {
		t.Error(err)
	}

	ts, err := dr.New("table_1", []benchtop.ColumnDef{
		{Key: "data"},
		{Key: "id"},
	})
	if err != nil {
		t.Error(err)
	}

	totalCount := 100
	jT, _ := ts.(*jTable.JSONTable)
	jDr, _ := dr.(*jsontable.JSONDriver)

	for i := range totalCount {
		key := fmt.Sprintf("key_%d", i)
		loc, err := jT.AddRow(benchtop.Row{Id: []byte(key), Data: map[string]any{
			"id":   key,
			"data": i,
		}})
		if err != nil {
			t.Error(err)
		}
		err = jDr.AddTableEntryInfo(nil, []byte(key), loc)
		if err != nil {
			t.Error(err)
		}
		_, ok := jDr.LocCache.Set(key, loc)
		if !ok {
			t.Fatalf("Failed to set loc: %#v", loc)
		}
	}

	count := 0
	r, err := jDr.ListTableKeys(jT.TableId)
	if err != nil {
		t.Error(err)
	}
	for i := range r {
		loc, err := jDr.LocCache.Get(context.Background(), string(i.Key))
		if err != nil {
			t.Error(err)
		}
		_, err = jT.GetRow(loc)
		if err != nil {
			t.Errorf("Get %s error: %s", string(i.Key), err)
		}
		count++
	}
	if count != totalCount {
		t.Errorf("incorrect return count %d", count)
	}

	var deleteCount = 0
	fmt.Println("TABLE ID: ", jT.TableId)
	keys, err := jDr.ListTableKeys(jT.TableId)
	if err != nil {
		t.Error(err)
	}
	i := 0
	for k := range keys {
		if i%3 == 0 {
			loc, err := jDr.LocCache.Get(context.Background(), string(k.Key))
			if err != nil {
				t.Error(err)
			}

			err = jDr.Pkv.Delete(benchtop.NewPosKey(jT.TableId, k.Key), nil)
			if err != nil {
				t.Fatal(err)
			}
			err = jT.DeleteRow(loc, k.Key)
			if err != nil {
				t.Errorf("delete %s error: %s", string(k.Key), err)
			}
			jDr.LocCache.Invalidate(string(k.Key))
			deleteCount++
		}
		i++
	}

	count = 0
	r, err = jDr.ListTableKeys(jT.TableId)
	if err != nil {
		t.Error(err)
	}
	for range r {
		count++
	}
	if totalCount-deleteCount != count {
		t.Errorf("incorrect return count after delete %d != %d", count, totalCount-deleteCount)
	}
	defer dr.Close()
}
