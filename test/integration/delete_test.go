package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/benchtop/util"
)

func TestDelete(t *testing.T) {
	dbname := "test.data" + util.RandomString(5)
	dr, err := bsontable.NewBSONDriver(dbname)
	if err != nil {
		t.Error(err)
	}

	ts, err := dr.New("table_1", []benchtop.ColumnDef{
		{Key: "data", Type: benchtop.Int64},
		{Key: "id", Type: benchtop.String},
	})

	if err != nil {
		t.Error(err)
	}

	totalCount := 100
	for i := 0; i < totalCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		err := ts.AddRow(benchtop.Row{Id: []byte(key), Data: map[string]any{
			"id":   key,
			"data": i,
		}})
		if err != nil {
			t.Error(err)
		}
	}

	count := 0
	r, err := ts.Keys()
	if err != nil {
		t.Error(err)
	}
	for i := range r {
		_, err := ts.GetRow(i.Key)
		if err != nil {
			t.Errorf("Get %s error: %s", string(i.Key), err)
		}
		count++
	}
	if count != totalCount {
		t.Errorf("incorrect return count %d", count)
	}

	deleteCount := 0
	keys, _ := ts.Keys()
	i := 0
	for k := range keys {
		if i%3 == 0 {
			err := ts.DeleteRow(k.Key)
			if err != nil {
				t.Errorf("delete %s error: %s", string(k.Key), err)
			}
			deleteCount++
			i++
		}
	}

	count = 0
	r, _ = ts.Keys()
	for range r {
		count++
	}

	if totalCount-deleteCount != count {
		t.Errorf("incorrect return count after delete %d != %d", count, totalCount-deleteCount)
	}

	defer dr.Close()
	os.RemoveAll(dbname)
}
