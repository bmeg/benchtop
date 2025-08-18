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

	columns := []benchtop.ColumnDef{{Key: "data", Type: benchtop.Bytes}}

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
	bT, _ := ot.(*bsontable.BSONTable)
	for key := range OTKEYS {
		if _, exists := randomIndexSet[count]; exists {

			pKey := benchtop.NewPosKey(bT.TableId, key.Key)
			val, closer, err := bT.Pb.Db.Get(pKey)
			if err != nil {
				if err != pebble.ErrNotFound {
					log.Errorf("Err on dr.Pb.Get for key %s in CacheLoader: %v", key.Key, err)
				}
				log.Errorln("ERR: ", err)
			}
			offset, size := benchtop.ParsePosValue(val)
			closer.Close()

			rOw, err := bT.GetRow(benchtop.RowLoc{Offset: offset, Size: size, Label: 0})
			if err != nil {
				b.Fatal(err)
			}
			selectedValues = append(selectedValues, rOw)
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
	os.RemoveAll(Bsonname)

}
