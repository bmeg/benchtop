package test

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable"
	"github.com/bmeg/benchtop/test/fixtures"
	"github.com/bmeg/benchtop/util"
)

// Simplified structures to mock gdbi/grip without importing everything
type MockVertex struct {
	ID    string
	Label string
	Data  map[string]any
}
type MockGraphElement struct {
	Vertex *MockVertex
	Graph  string
}

func BenchmarkGripFullPipeline(b *testing.B) {
	dbPath := "bench_repro_full_db_" + util.RandomString(5)
	_ = os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	driver, err := jsontable.NewJSONDriver(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	// driver_test.go (arrow)
	// tid, _ := drv2.LookupTableID("e_knows") // This line is commented out as drv2 is not defined
	// t2, err = drv2.Get(tid) // This line is commented out as drv2 is not defined
	defer driver.Close()
	jDriver, _ := driver.(*jsontable.JSONDriver)

	columns := []benchtop.ColumnDef{{Key: "data"}}
	tableName := "v_test_label"
	jDriver.New(tableName, columns)

	val := fixtures.GenerateRandomBytes(1024)

	b.ResetTimer()

	// scale_test.go
	// ch <- benchtop.Row{Id: []byte(k), TableID: tid, Data: v}
	// Need to get tid first.
	// But channel is consumed by BulkLoad.
	// BulkLoad call is AFTER loop? No, concurrently?
	// The snippet above shows BulkLoad called with channel.
	// We need tableID inside loop?
	// "go func() { ... ch <- Row... }"
	// Pass tid to goroutine.
	// 1. Client Stream (Input)
	clientStream := make(chan *MockGraphElement, 100)

	// 2. Server BulkAdd (reads clientStream, validates, pushes to elementStream)
	elementStream := make(chan *MockGraphElement, 100)
	var serverWG sync.WaitGroup
	serverWG.Add(1)
	go func() {
		defer serverWG.Done()
		defer close(elementStream)
		for elem := range clientStream {
			// Simulate Validation logic
			if elem.Vertex.ID == "" {
				continue
			}
			elementStream <- elem
		}
	}()

	// 3. Graph BulkAdd (reads elementStream, splits to insert/index streams)
	insertStream := make(chan *MockGraphElement, 100)
	indexStream := make(chan *benchtop.Row, 100)
	var graphWG sync.WaitGroup
	graphWG.Add(2) // Two consumers for the split streams

	// Graph Splitter
	go func() {
		defer close(insertStream)
		defer close(indexStream)
		for elem := range elementStream {
			insertStream <- elem
			if elem.Vertex != nil {
				tName := "v_" + elem.Vertex.Label
				tid, _ := jDriver.LookupTableID(tName)
				row := benchtop.Row{
					Id:      []byte(elem.Vertex.ID),
					TableID: tid,
					Data:    elem.Vertex.Data,
				}
				indexStream <- &row
			}
		}
	}()

	// Consumer 1: InsertVertex (Simulated)
	go func() {
		defer graphWG.Done()
		for range insertStream {
		}
	}()

	// Consumer 2: Index (BulkLoad)
	tid, _ := jDriver.LookupTableID(tableName)
	go func() {
		defer graphWG.Done()
		// scale_test.go
		// tid, _ := jsonDriver.LookupTableID(Jsonname) // This line is commented out as jsonDriver and Jsonname are not defined
		// err = jsonDriver.BulkLoad(tid, ch) // This line is commented out as jsonDriver and ch are not defined
		_ = jDriver.BulkLoad(tid, indexStream)
	}()

	// Producer
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("v%010d", i)
		elem := &MockGraphElement{
			Vertex: &MockVertex{
				ID:    key,
				Label: "test_label",
				Data:  map[string]any{"data": val, "id": key},
			},
			Graph: "test-graph",
		}
		clientStream <- elem
	}
	close(clientStream)

	serverWG.Wait()
	graphWG.Wait()
}
