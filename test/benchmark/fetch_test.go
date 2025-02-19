package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/test/fixtures"
	"github.com/bmeg/benchtop/util"
)

const (
	NumKeys   = 100000
	ValueSize = 5024
)

func BenchmarkFetch(b *testing.B) {
	var compactbsoname = "test.bson" + util.RandomString(5)
	defer os.Remove(compactbsoname) // Clean up

	b.Log("BenchmarkScaleWriteBson start")

	compactbsonDriver, err := benchtop.NewBSONDriver(compactbsoname)
	if err != nil {
		b.Fatal(err)
	}

	columns := []benchtop.ColumnDef{{Name: "data", Type: benchtop.Bytes}}

	compactbsonTable, err := compactbsonDriver.New(compactbsoname, columns)
	if err != nil {
		b.Fatal(err)
	}

	inputChan := make(chan benchtop.Entry, 100)
	go func() {
		count := 0
		for j := 0; j < NumKeys; j++ {
			key := []byte(fmt.Sprintf("key_%d", j))
			value := fixtures.GenerateRandomBytes(ValueSize)
			inputChan <- benchtop.Entry{Key: key, Value: map[string]any{"data": value}}
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

	b.Log("KEYS", keys)

	outStruct := compactbsonTable.Fetch(keys, 5)
	keyCount := 0
	for _ = range outStruct {
		keyCount++
	}
	b.Log("KEY COUNT: ", keyCount)
}
