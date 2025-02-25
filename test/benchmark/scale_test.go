package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/benchtop/pebblebsontable"
	"github.com/bmeg/benchtop/test/fixtures"
	"github.com/bmeg/benchtop/util"
)

var Bsonname = "test.bson" + util.RandomString(5)
var bsonTable *bsontable.BSONTable
var bsonDriver *bsontable.BSONDriver

const (
	scalenumKeys   = 100000
	scalevalueSize = 5024
)

func BenchmarkScaleWriteBson(b *testing.B) {
	b.Log("BenchmarkScaleWriteBson start")

	var err error
	if bsonDriver == nil {
		driver, err := bsontable.NewBSONDriver(Bsonname)
		if err != nil {
			b.Fatal(err)
		}
		var ok bool
		bsonDriver, ok = driver.(*bsontable.BSONDriver)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.BSONDriver")
		}
	}

	columns := []benchtop.ColumnDef{{Name: "data", Type: benchtop.Bytes}}

	if bsonTable == nil {
		table, err := bsonDriver.New(Bsonname, columns)
		if err != nil {
			b.Fatal(err)
		}

		var ok bool
		bsonTable, ok = table.(*bsontable.BSONTable)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.BSONDriver")
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		inputChan := make(chan benchtop.Entry, 100)
		go func() {
			for j := 0; j < scalenumKeys; j++ {
				key := []byte(fmt.Sprintf("key_%d", j))
				value := fixtures.GenerateRandomBytes(scalevalueSize)
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
	var err error
	if bsonDriver == nil {
		driver, err := bsontable.NewBSONDriver(Bsonname)
		if err != nil {
			b.Fatal(err)
		}
		var ok bool
		bsonDriver, ok = driver.(*bsontable.BSONDriver)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.BSONDriver")
		}
	}

	ot, err := bsonDriver.Get(Bsonname)
	if err != nil {
		b.Log(err)
	}
	defer ot.Close()

	randomIndexSet, err := fixtures.GetRandomUniqueIntegers(200000, 1000000)
	selectedValues := make([]map[string]any, 0, len(randomIndexSet))
	count := 0
	b.ResetTimer()

	OTKEYS, _ := ot.Keys()
	for key := range OTKEYS {
		if _, exists := randomIndexSet[count]; exists {
			val, err := ot.Get(key.Key)
			if err != nil {
				b.Fatal(err)
			}
			selectedValues = append(selectedValues, val)
		}
		count++
	}
	b.Log("READS:", len(selectedValues), "COUNT: ", count)

}

func BenchmarkRandomKeysBson(b *testing.B) {
	var err error
	if bsonDriver == nil {
		driver, err := bsontable.NewBSONDriver(Bsonname)
		if err != nil {
			b.Fatal(err)
		}
		var ok bool
		bsonDriver, ok = driver.(*bsontable.BSONDriver)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.BSONDriver")
		}
	}
	ot, err := bsonDriver.Get(Bsonname)
	if err != nil {
		b.Log(err)
	}
	defer ot.Close()

	randomIndexSet, err := fixtures.GetRandomUniqueIntegers(200000, 1000000)
	selectedValues := make([][]byte, 0, len(randomIndexSet))
	count := 0
	b.ResetTimer()

	OTKEYS, _ := ot.Keys()
	for key := range OTKEYS {
		if _, exists := randomIndexSet[count]; exists {
			selectedValues = append(selectedValues, key.Key)
		}
		count++
	}
	b.Log("READS: ", len(selectedValues), "COUNT: ", count)
	os.RemoveAll(Bsonname)

}

var Pebblename = "test.pebble" + util.RandomString(5)
var pebbleTable *pebblebsontable.PebbleBSONTable
var pebbleDriver *pebblebsontable.PebbleBSONDriver

func BenchmarkScaleWritePebble(b *testing.B) {
	var err error
	if pebbleDriver == nil {
		driver, err := pebblebsontable.NewPebbleBSONDriver(Pebblename)
		if err != nil {
			b.Fatal(err)
		}
		var ok bool
		pebbleDriver, ok = driver.(*pebblebsontable.PebbleBSONDriver)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.BSONDriver")
		}
	}

	columns := []benchtop.ColumnDef{{Name: "data", Type: benchtop.Bytes}}

	if pebbleTable == nil {
		table, err := pebbleDriver.New(Pebblename, columns)
		if err != nil {
			b.Fatal(err)
		}

		var ok bool
		pebbleTable, ok = table.(*pebblebsontable.PebbleBSONTable)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.BSONDriver")
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		inputChan := make(chan benchtop.Entry, 100)
		go func() {
			for j := 0; j < scalenumKeys; j++ {
				key := append(benchtop.NewPosKeyPrefix(pebbleTable.TableId), []byte(fmt.Sprintf("key_%d", j))...)
				value := fixtures.GenerateRandomBytes(scalevalueSize)
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
	var err error
	if pebbleDriver == nil {
		driver, err := pebblebsontable.NewPebbleBSONDriver(Pebblename)
		if err != nil {
			b.Fatal(err)
		}
		var ok bool
		pebbleDriver, ok = driver.(*pebblebsontable.PebbleBSONDriver)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.BSONDriver")
		}
	}

	ot, err := pebbleDriver.Get(Pebblename)
	if err != nil {
		b.Log(err)
	}
	defer ot.Close()

	randomIndexSet, err := fixtures.GetRandomUniqueIntegers(200000, 1000000)

	b.ResetTimer()

	OTKEYS, err := ot.Keys()
	if err != nil {
		b.Log(err)
	}
	var selectedValues []map[string]any
	count := 0

	for key := range OTKEYS {
		if _, exists := randomIndexSet[count]; exists {
			val, err := ot.Get(key.Key)
			if err != nil {
				b.Log("ERR: ", err)
			}
			selectedValues = append(selectedValues, val)
		}
		count++
	}
	b.Log("READS: ", len(selectedValues), "COUNT: ", count)

}

func BenchmarkRandomKeysPebble(b *testing.B) {
	var err error
	if pebbleDriver == nil {
		driver, err := pebblebsontable.NewPebbleBSONDriver(Pebblename)
		if err != nil {
			b.Fatal(err)
		}
		var ok bool
		pebbleDriver, ok = driver.(*pebblebsontable.PebbleBSONDriver)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.BSONDriver")
		}
	}

	ot, err := pebbleDriver.Get(Pebblename)
	if err != nil {
		b.Log(err)
	}
	defer ot.Close()

	randomIndexSet, err := fixtures.GetRandomUniqueIntegers(200000, 1000000)
	selectedValues := make([][]byte, 0, len(randomIndexSet))
	count := 0

	b.ResetTimer()

	OTKEYS, _ := ot.Keys()
	for key := range OTKEYS {
		if _, exists := randomIndexSet[count]; exists {
			selectedValues = append(selectedValues, key.Key)
		}
		count++
	}
	b.Log("KEYS: ", len(selectedValues), "COUNT: ", count)
	os.RemoveAll(Pebblename)

}
