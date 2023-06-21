package test

import (
	"fmt"
	"testing"

	"github.com/bmeg/benchtop"
)

func TestDelete(t *testing.T) {

	ts, err := benchtop.Create("test.data", []benchtop.ColumnDef{
		{Path: "data", Type: benchtop.Int64},
		{Path: "id", Type: benchtop.String},
	})

	if err != nil {
		t.Error(err)
	}

	totalCount := 100
	offsets := []int64{}
	for i := 0; i < totalCount; i++ {
		o, err := ts.Add(map[string]any{
			"id":   fmt.Sprintf("key_%d", i),
			"data": i,
		})
		if err != nil {
			t.Error(err)
		}
		offsets = append(offsets, o)
	}

	count := 0
	r, err := ts.ListOffsets()
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
	for i := range offsets {
		if i%3 == 0 {
			err := ts.Delete(offsets[i])
			if err != nil {
				t.Error(err)
			}
			deleteCount++
		}
	}

	count = 0
	r, _ = ts.ListOffsets()
	for range r {
		count++
	}

	if totalCount-deleteCount != count {
		t.Errorf("incorrect return count after delete %d != %d", count, totalCount-deleteCount)
	}
}
