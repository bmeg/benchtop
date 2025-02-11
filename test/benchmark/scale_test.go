package test

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/util"
)

const (
	NumKeys   = 1000000
	ValueSize = 5024
)

func GenerateRandomBytes(size int) []byte {
	b := make([]byte, size)
	rand.Read(b)
	return b
}

func getRandomUniqueIntegers(count, max int) (map[int]struct{}, error) {
	uniqueNumbers := make(map[int]struct{})
	for len(uniqueNumbers) < count {
		nBig, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
		if err != nil {
			return nil, err
		}
		num := int(nBig.Int64())

		if _, exists := uniqueNumbers[num]; !exists {
			uniqueNumbers[num] = struct{}{}
		}
	}

	return uniqueNumbers, nil
}

var Bsonname = "test.bson" + util.RandomString(5)
var bsonTable *benchtop.BSONTable
var bsonDriver *benchtop.BSONDriver

func BenchmarkScaleWriteBson(b *testing.B) {
	b.Log("BenchmarkScaleWriteBson start")

	var err error
	if bsonDriver == nil {
		driver, err := benchtop.NewBSONDriver(Bsonname)
		if err != nil {
			b.Fatal(err)
		}
		var ok bool
		bsonDriver, ok = driver.(*benchtop.BSONDriver)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.BSONDriver")
		}
	}

	columns := []benchtop.ColumnDef{{Path: "data", Type: benchtop.Bytes}}

	if bsonTable == nil {
		table, err := bsonDriver.New(Bsonname, columns)
		if err != nil {
			b.Fatal(err)
		}

		var ok bool
		bsonTable, ok = table.(*benchtop.BSONTable)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.BSONDriver")
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		inputChan := make(chan benchtop.Entry, 100)
		go func() {
			for j := 0; j < NumKeys; j++ {
				key := []byte(fmt.Sprintf("key_%d", j))
				value := GenerateRandomBytes(ValueSize)
				inputChan <- benchtop.Entry{Key: key, Value: map[string]any{"data": value}}
			}
			close(inputChan)
		}()

		err = bsonTable.Load(inputChan)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRandomReadBson(b *testing.B) {
	driver, err := benchtop.NewBSONDriver(Bsonname)
	if err != nil {
		b.Fatal(err)
	}
	defer driver.Close()

	ot, err := driver.Get(Bsonname)
	if err != nil {
		b.Log(err)
	}

	randomIndexSet, err := getRandomUniqueIntegers(200000, 1000000)
	selectedValues := make([]map[string]any, 0, len(randomIndexSet))
	count := 0
	b.ResetTimer()

	OTKEYS, _ := ot.Keys()
	for key := range OTKEYS {
		if _, exists := randomIndexSet[count]; exists {
			val, err := ot.Get(key)
			if err != nil {
				b.Fatal(err)
			}
			selectedValues = append(selectedValues, val)
		}
		count++
	}
	if len(selectedValues) != len(randomIndexSet) {
		b.Logf("Error: %#v != %#v\n", selectedValues, randomIndexSet)
	}
}

func BenchmarkRandomKeysBson(b *testing.B) {
	driver, err := benchtop.NewBSONDriver(Bsonname)
	if err != nil {
		b.Fatal(err)
	}
	defer driver.Close()

	ot, err := driver.Get(Bsonname)
	if err != nil {
		b.Log(err)
	}

	randomIndexSet, err := getRandomUniqueIntegers(200000, 1000000)
	selectedValues := make([][]byte, 0, len(randomIndexSet))
	count := 0
	b.ResetTimer()

	OTKEYS, _ := ot.Keys()
	for key := range OTKEYS {
		if _, exists := randomIndexSet[count]; exists {
			selectedValues = append(selectedValues, key)
		}
		count++
	}
	if len(selectedValues) != len(randomIndexSet) {
		b.Logf("Error: %#v != %#v\n", selectedValues, randomIndexSet)
	}
}

var Pebblename = "test.pebble" + util.RandomString(5)
var pebbleTable *benchtop.PebbleBSONTable
var pebbleDriver *benchtop.PebbleBSONDriver

func BenchmarkScaleWritePebble(b *testing.B) {
	var err error
	if pebbleDriver == nil {
		driver, err := benchtop.NewPebbleBSONDriver(Pebblename)
		if err != nil {
			b.Fatal(err)
		}
		var ok bool
		pebbleDriver, ok = driver.(*benchtop.PebbleBSONDriver)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.BSONDriver")
		}
	}

	columns := []benchtop.ColumnDef{{Path: "data", Type: benchtop.Bytes}}

	if pebbleTable == nil {
		table, err := pebbleDriver.New(Pebblename, columns)
		if err != nil {
			b.Fatal(err)
		}

		var ok bool
		pebbleTable, ok = table.(*benchtop.PebbleBSONTable)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.BSONDriver")
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		inputChan := make(chan benchtop.Entry, 100)
		go func() {
			for j := 0; j < NumKeys; j++ {
				key := append(benchtop.NewPosKeyPrefix(pebbleTable.TableId), []byte(fmt.Sprintf("key_%d", j))...)
				value := GenerateRandomBytes(ValueSize)
				inputChan <- benchtop.Entry{Key: key, Value: map[string]any{"data": value}}
			}
			close(inputChan)
		}()

		err = pebbleTable.Load(inputChan)
		if err != nil {
			b.Fatal(err)
		}

	}
}

func BenchmarkRandomReadPebble(b *testing.B) {
	driver, err := benchtop.NewPebbleBSONDriver(Pebblename)
	if err != nil {
		b.Fatal(err)
	}
	defer driver.Close()

	ot, err := driver.Get(Pebblename)
	if err != nil {
		b.Log(err)
	}

	randomIndexSet, err := getRandomUniqueIntegers(200000, 1000000)

	OTKEYS, err := ot.Keys()
	if err != nil {
		b.Log(err)
	}
	var selectedValues []map[string]any
	count := 0

	b.ResetTimer()
	for key := range OTKEYS {
		if _, exists := randomIndexSet[count]; exists {
			val, _ := ot.Get(key)
			selectedValues = append(selectedValues, val)
		}
		count++
	}
	b.Log("LEN SEL VALUES", len(selectedValues))

}

func BenchmarkRandomKeysPebble(b *testing.B) {
	driver, err := benchtop.NewPebbleBSONDriver(Pebblename)
	if err != nil {
		b.Fatal(err)
	}
	defer driver.Close()

	ot, err := driver.Get(Pebblename)
	if err != nil {
		b.Log(err)
	}

	randomIndexSet, err := getRandomUniqueIntegers(200000, 1000000)
	selectedValues := make([][]byte, 0, len(randomIndexSet))
	count := 0
	b.ResetTimer()

	OTKEYS, _ := ot.Keys()
	for key := range OTKEYS {
		if _, exists := randomIndexSet[count]; exists {
			selectedValues = append(selectedValues, key)
		}
		count++
	}
	if len(selectedValues) != len(randomIndexSet) {
		b.Logf("Error: %#v != %#v\n", selectedValues, randomIndexSet)
	}
}
