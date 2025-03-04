package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/benchtop/test/fixtures"
	"github.com/bmeg/benchtop/util"
)

const (
	fetchNumKeys   = 100000
	fetchValueSize = 5024
)

func BenchmarkFetch(b *testing.B) {
	var fetchname = "test.bson" + util.RandomString(5)
	defer os.Remove(fetchname) // Clean up

	b.Log("BenchmarkScaleWriteBson start")

	compactbsonDriver, err := bsontable.NewBSONDriver(fetchname)
	if err != nil {
		b.Fatal(err)
	}

	columns := []benchtop.ColumnDef{{Name: "data", Type: benchtop.Bytes}}

	compactbsonTable, err := compactbsonDriver.New(fetchname, columns)
	if err != nil {
		b.Fatal(err)
	}

	inputChan := make(chan benchtop.Row, 100)
	go func() {
		count := 0
		for j := 0; j < fetchNumKeys; j++ {
			key := []byte(fmt.Sprintf("key_%d", j))
			value := fixtures.GenerateRandomBytes(fetchValueSize)
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

	keys, err := compactbsonTable.Keys()
	if err != nil {
		b.Fatal(err)
	}

	outStruct := compactbsonTable.Fetch(keys, 5)
	keyCount := 0
	for _ = range outStruct {
		//b.Log("KEY: ", keys)
		keyCount++
	}
	b.Log("KEY COUNT: ", keyCount)
	os.RemoveAll(fetchname)
}
