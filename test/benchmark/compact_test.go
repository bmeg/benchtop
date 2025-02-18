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
	NumKeys       = 100000
	ValueSize     = 5024
	NumDeleteKeys = 20000
)

func BenchmarkCompactBson(b *testing.B) {
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

	randomIndexSet, err := fixtures.GetRandomUniqueIntegers(NumDeleteKeys, NumKeys)
	if err != nil {
		b.Fatal(err)
	}

	count := 0
	deleted := 0
	for key := range keys {
		if _, exists := randomIndexSet[count]; exists {
			if err := compactbsonTable.Delete(key); err != nil {
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
	if keyCount != (NumKeys - NumDeleteKeys) {
		b.Fatalf("Keycount %d not equal expected %d", keyCount, (NumKeys - NumDeleteKeys))
	}

	b.Logf("Keys after compaction: %d", keyCount)
	os.RemoveAll(compactbsoname)
}
