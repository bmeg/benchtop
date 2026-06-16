package test

import (
	"fmt"
	"os"
	"sync"
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
		jsonTable, ok = table.(*jTable.JSONTable)
		if !ok {
			b.Fatal("Failed to assert type *benchtop.JSONDriver")
		}
	}

	b.ResetTimer()

	// Start producer
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		ch := make(chan *benchtop.Row, 100)

		// Start consumer
		tid, _ := jsonDriver.LookupTableID(Jsonname)
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = jsonDriver.BulkLoad(tid, ch)
			if err != nil {
				b.Error(err)
			}
		}()

		k := fmt.Sprintf("%016d", i)
		v := map[string]interface{}{}
		for j := 0; j < 10; j++ {
			v[fmt.Sprintf("key_%d", j)] = fmt.Sprintf("value_%d", j)
		}
		ch <- &benchtop.Row{Id: []byte(k), TableID: tid, Data: v}
		close(ch)
		wg.Wait()
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

	tid, _ := jsonDriver.LookupTableID(Jsonname)
	ot, err := jsonDriver.Get(tid)
	if err != nil {
		b.Log(err)
	}
	defer ot.Close()

	randomIndexSet, err := fixtures.GetRandomUniqueIntegers(200000, 1000000)
	selectedValues := make([]map[string]any, 0, len(randomIndexSet))
	count := 0
	b.ResetTimer()
	jT, _ := ot.(*jTable.JSONTable)
	OTKEYS, _ := jsonDriver.ListTableKeys(jT.TableId)
	for key := range OTKEYS {
		if _, exists := randomIndexSet[count]; exists {

			pKey := benchtop.NewPosKey(jT.TableId, key.Key)
			val, closer, err := jsonDriver.Pkv.Db.Get(pKey)
			if err != nil {
				if err != pebble.ErrNotFound {
					log.Errorf("Err on dr.Pb.Get for key %s in CacheLoader: %v", key.Key, err)
				}
				log.Errorln("ERR: ", err)
			}
			loc := benchtop.DecodeRowLoc(val)
			closer.Close()

			// driver_test.go
			// The following lines are commented out because 'drv2' is not defined in this scope,
			// and 't2' is not declared. This snippet appears to be from a different test file.
			// tid, _ := drv2.LookupTableID("e_knows")
			// t2, err = drv2.Get(tid)
			rOw, err := jT.GetRow(loc)
			if err != nil {
				b.Fatal(err)
			}
			selectedValues = append(selectedValues, rOw)
		}
		count++
	}
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
	tid_get, _ := jsonDriver.LookupTableID(Jsonname)
	ot, err := jsonDriver.Get(tid_get)
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

	jT, _ := ot.(*jTable.JSONTable)
	OTKEYS, _ := jsonDriver.ListTableKeys(jT.TableId)
	for key := range OTKEYS {
		if _, exists := randomIndexSet[count]; exists {
			selectedValues = append(selectedValues, key.Key)
		}
		count++
	}
	b.Log("READS: ", len(selectedValues), "COUNT: ", count)
	os.RemoveAll(Jsonname)

}
