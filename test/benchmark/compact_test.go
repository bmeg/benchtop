package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/akrylysov/pogreb"
	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/benchtop/test/fixtures"
	"github.com/bmeg/benchtop/util"
)

const (
	numKeys       = 1000
	valueSize     = 5024
	NumDeleteKeys = 200
)

func BenchmarkCompactBson(b *testing.B) {
	var compactbsoname = "test.bson" + util.RandomString(5)
	defer os.RemoveAll(compactbsoname)
	pogrebName := compactbsoname + "pogreb"
	defer os.RemoveAll(pogrebName)

	pg, err := pogreb.Open(pogrebName, nil)
	if err != nil {
		b.Error(err)
	}

	b.Log("BenchmarkScaleWriteBson start")

	compactbsonDriver, err := bsontable.NewBSONDriver(compactbsoname, pg)
	if err != nil {
		b.Fatal(err)
	}

	columns := []benchtop.ColumnDef{{Key: "data", Type: benchtop.Bytes}}

	compactbsonTable, err := compactbsonDriver.New(compactbsoname, columns)
	if err != nil {
		b.Fatal(err)
	}

	inputChan := make(chan benchtop.Row, 100)
	go func() {
		count := 0
		for j := 0; j < numKeys; j++ {
			key := []byte(fmt.Sprintf("key_%d", j))
			value := fixtures.GenerateRandomBytes(valueSize)
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

	randomIndexSet, err := fixtures.GetRandomUniqueIntegers(NumDeleteKeys, numKeys)
	if err != nil {
		b.Fatal(err)
	}

	count := 0
	deleted := 0
	for key := range keys {
		if _, exists := randomIndexSet[count]; exists {
			if err := compactbsonTable.DeleteRow(key.Key); err != nil {
				b.Fatal(err)
			}
			deleted++
		}
		count++
	}
	b.Logf("Deleted %d keys", deleted)

	b.Log("start compact")
	b.ResetTimer()

	if err := compactbsonTable.Compact(); err != nil {
		b.Fatal(err)
	}

	keysAfterCompact, err := compactbsonTable.Keys()
	if err != nil {
		b.Fatal(err)
	}

	keyCount := 0
	for _ = range keysAfterCompact {
		keyCount++
	}
	if keyCount != (numKeys - NumDeleteKeys) {
		b.Fatalf("Keycount %d not equal expected %d", keyCount, (numKeys - NumDeleteKeys))
	}

	b.Logf("Keys after compaction: %d", keyCount)
}
