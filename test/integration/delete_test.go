package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable"
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
	bT, _ := ts.(*jsontable.JSONTable)
	for i := 0; i < totalCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		loc, err := bT.AddRow(benchtop.Row{Id: []byte(key), Data: map[string]any{
			"id":   key,
			"data": i,
		}})
		if err != nil {
			t.Error(err)
		}
		err = bT.AddTableEntryInfo(nil, []byte(key), *loc)
		if err != nil {
			t.Error(err)
		}
	}

	count := 0
	r, err := bT.Keys()
	if err != nil {
		t.Error(err)
	}
	for i := range r {
		offset, size, err := bT.GetBlockPos(i.Key)
		if err != nil {
			t.Error(err)
		}
		_, err = bT.GetRow(benchtop.RowLoc{Offset: offset, Size: size, Label: uint16(0)})
		if err != nil {
			t.Errorf("Get %s error: %s", string(i.Key), err)
		}
		count++
	}
	if count != totalCount {
		t.Errorf("incorrect return count %d", count)
	}

	var deleteCount = 0
	keys, err := bT.Keys()
	if err != nil {
		t.Error(err)
	}
	i := 0
	for k := range keys {
		if i%3 == 0 {
			offset, size, err := bT.GetBlockPos(k.Key)
			if err != nil {
				t.Error(err)
			}
			err = bT.DeleteRow(benchtop.RowLoc{Offset: offset, Size: size, Label: bT.TableId}, k.Key)
			if err != nil {
				t.Errorf("delete %s error: %s", string(k.Key), err)
			}
			deleteCount++
		}
		i++
	}

	count = 0
	r, err = bT.Keys()
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
