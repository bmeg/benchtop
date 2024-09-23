package test

import (
	"fmt"
	"testing"

	"github.com/bmeg/benchtop"
)

func TestDelete(t *testing.T) {

	dr, err := benchtop.NewBSONDriver("test.data")

	ts, err := dr.New("test.data", []benchtop.ColumnDef{
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
			t.Error(err)
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
				t.Error(err)
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
}
