package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable"
	jTable "github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/benchtop/test/fixtures"
	"github.com/bmeg/benchtop/util"
	"github.com/bmeg/grip/log"
	"github.com/cockroachdb/pebble"
)

var Jsonname = "test.json" + util.RandomString(5)
var jsonTable *jTable.JSONTable
var jsonDriver *jsontable.JSONDriver

const (
	scalenumKeys   = 100000
	scalevalueSize = 5024
)

func BenchmarkScaleWriteJson(b *testing.B) {
	b.Log("BenchmarkScaleWriteJson start")

	var err error
	if jsonDriver == nil {
		driver, err := jsontable.NewJSONDriver(Jsonname)
		if err != nil {
			b.Fatal(err)
		}
		var ok bool
		jsonDriver, ok = driver.(*jsontable.JSONDriver)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.JSONDriver")
		}
	}

	columns := []benchtop.ColumnDef{{Key: "data"}}

	if jsonTable == nil {
		table, err := jsonDriver.New(Jsonname, columns)
		if err != nil {
			b.Fatal(err)
		}

		var ok bool
		jsonTable, ok = table.(*jsontable.JSONTable)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.JSONDriver")
		}
	}

	b.ResetTimer()

	for b.Loop() {
		inputChan := make(chan benchtop.Row, 100)
		go func() {
			for j := range scalenumKeys {
				key := []byte(fmt.Sprintf("key_%d", j))
				value := fixtures.GenerateRandomBytes(scalevalueSize)
				inputChan <- benchtop.Row{Id: key, Data: map[string]any{"data": value}}
			}
			close(inputChan)
		}()

		err = jsonTable.Load(inputChan)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRandomReadJson(b *testing.B) {
	var err error
	if jsonDriver == nil {
		driver, err := jsontable.NewJSONDriver(Jsonname)
		if err != nil {
			b.Fatal(err)
		}
		var ok bool
		jsonDriver, ok = driver.(*jsontable.JSONDriver)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.JSONDriver")
		}
	}

	ot, err := jsonDriver.Get(Jsonname)
	if err != nil {
		b.Log(err)
	}
	defer ot.Close()

	randomIndexSet, err := fixtures.GetRandomUniqueIntegers(200000, 1000000)
	selectedValues := make([]map[string]any, 0, len(randomIndexSet))
	count := 0
	b.ResetTimer()

	OTKEYS, _ := ot.Keys()
	jT, _ := ot.(*jsontable.JSONTable)
	for key := range OTKEYS {
		if _, exists := randomIndexSet[count]; exists {

			pKey := benchtop.NewPosKey(jT.TableId, key.Key)
			val, closer, err := jT.Pb.Db.Get(pKey)
			if err != nil {
				if err != pebble.ErrNotFound {
					log.Errorf("Err on dr.Pb.Get for key %s in CacheLoader: %v", key.Key, err)
				}
				log.Errorln("ERR: ", err)
			}
			loc := benchtop.DecodeRowLoc(val)
			closer.Close()

			rOw, err := jT.GetRow(loc)
			if err != nil {
				b.Fatal(err)
			}
			selectedValues = append(selectedValues, rOw)
		}
		count++
	}
	b.Log("READS:", len(selectedValues), "COUNT: ", count)

}

func BenchmarkRandomKeysJson(b *testing.B) {
	var err error
	if jsonDriver == nil {
		driver, err := jsontable.NewJSONDriver(Jsonname)
		if err != nil {
			b.Fatal(err)
		}
		var ok bool
		jsonDriver, ok = driver.(*jsontable.JSONDriver)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.JSONDriver")
		}
	}
	ot, err := jsonDriver.Get(Jsonname)
	if err != nil {
		b.Log(err)
	}
	defer ot.Close()

	randomIndexSet, err := fixtures.GetRandomUniqueIntegers(200000, 1000000)
	if err != nil {
		b.Log(err)
	}
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
	os.RemoveAll(Jsonname)

}
