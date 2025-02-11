package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/util"
)

func TestPebbleDelete(t *testing.T) {
	dbname := "test.data" + util.RandomString(5)
	dr, err := benchtop.NewPebbleBSONDriver(dbname)
	if err != nil {
		t.Error(err)
	}

	ts, err := dr.New("table_2", []benchtop.ColumnDef{
		{Path: "data", Type: benchtop.Int64},
		{Path: "id", Type: benchtop.String},
	})

	if err != nil {
		t.Error(err)
	}

	totalCount := 100
	for i := 0; i < totalCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		err := ts.Add([]byte(key), map[string]any{
			"id":   key,
			"data": i,
		})
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
		_, err := ts.Get(i)
		if err != nil {
			t.Errorf("Get %s error: %s", i, err)
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
			err := ts.Delete(k)
			if err != nil {
				t.Errorf("delete %s error: %s", k, err)
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

	dr.Close()
	os.RemoveAll(dbname)
}
