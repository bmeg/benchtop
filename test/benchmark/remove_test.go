package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/benchtop/test/fixtures"
	"github.com/bmeg/benchtop/util"
	"github.com/bmeg/grip/log"
	"github.com/cockroachdb/pebble"
)

const (
	removeNumKeys   = 1000
	removeValueSize = 5024
)

func BenchmarkRemove(b *testing.B) {
	var removename = "test.bson" + util.RandomString(5)
	defer os.RemoveAll(removename) // Clean up
	b.Log("BenchmarkScaleWriteBson start")

	compactbsonDriver, err := bsontable.NewBSONDriver(removename)
	if err != nil {
		b.Fatal(err)
	}

	columns := []benchtop.ColumnDef{{Key: "data", Type: benchtop.Bytes}}

	compactbsonTable, err := compactbsonDriver.New(removename, columns)
	if err != nil {
		b.Fatal(err)
	}

	inputChan := make(chan benchtop.Row, 100)
	go func() {
		count := 0
		for j := 0; j < removeNumKeys; j++ {
			key := []byte(fmt.Sprintf("key_%d", j))
			value := fixtures.GenerateRandomBytes(removeValueSize)
			inputChan <- benchtop.Row{Id: key, Data: map[string]any{"data": value}}
			count++
		}
		b.Logf("Inserted %d entries into inputChan", count)
		close(inputChan)
	}()

	b.Log("start load")
	if err := compactbsonTable.Load(inputChan); err != nil {
		b.Fatal(err)
	}
	b.Log("Load completed successfully")

	bT, _ := compactbsonTable.(*bsontable.BSONTable)
	pKey := benchtop.NewPosKey(bT.TableId, []byte("key_5"))
	val, closer, err := bT.Pb.Db.Get(pKey)
	if err != nil {
		if err != pebble.ErrNotFound {
			log.Errorf("Err on dr.Pb.Get for key %s in CacheLoader: %v", pKey, err)
		}
		log.Errorln("ERR: ", err)
	}
	closer.Close()
	offset, size := benchtop.ParsePosValue(val)

	data, err := compactbsonTable.GetRow(benchtop.RowLoc{Offset: offset, Size: size, Label: 0})
	b.Log("DATA BEFORE: ", data)

	if len(data) == 0 {
		b.Fatal("Expected data to be in key_5 but none was found")
	}

	keys, err := compactbsonTable.Keys()
	if err != nil {
		b.Fatal(err)
	}

	outStruct := compactbsonTable.Remove(keys, 5)
	keyCount := 0
	for _ = range outStruct {
		keyCount++
	}

	keys, err = compactbsonTable.Keys()
	if err != nil {
		b.Fatal(err)
	}

	data, err = compactbsonTable.GetRow(benchtop.RowLoc{Offset: offset, Size: size, Label: 0})
	b.Log("DATA AFTER: ", data)
	if len(data) != 0 {
		b.Fatalf("Expected data to be empty for key_5 but %#v was found\n", data)
	}

	for key := range keys {
		b.Error("Unexpected Key: ", key)
	}

	scaChan := compactbsonTable.Scan(true, nil)
	for elem := range scaChan {
		fmt.Println("ELEM: ", elem)
	}
}
