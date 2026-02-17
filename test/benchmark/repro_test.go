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
	defer driver.Close()
	jDriver, _ := driver.(*jsontable.JSONDriver)

	columns := []benchtop.ColumnDef{{Key: "data"}}
	tableName := "v_test_label"
	jDriver.New(tableName, columns)

	val := fixtures.GenerateRandomBytes(1024)

	b.ResetTimer()

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
	indexStream := make(chan benchtop.Row, 100)
	var graphWG sync.WaitGroup
	graphWG.Add(2) // Two consumers for the split streams

	// Graph Splitter
	go func() {
		defer close(insertStream)
		defer close(indexStream)
		for elem := range elementStream {
			insertStream <- elem
			if elem.Vertex != nil {
				indexStream <- benchtop.Row{
					Id:        []byte(elem.Vertex.ID),
					TableName: "v_" + elem.Vertex.Label,
					Data:      elem.Vertex.Data,
				}
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
	go func() {
		defer graphWG.Done()
		_ = jDriver.BulkLoad(tableName, indexStream)
	}()

	// Producer
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
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
