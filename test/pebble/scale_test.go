package test

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/util"
)

const (
	numKeys   = 40000
	valueSize = 1024
)

func generateRandomBytes(size int) []byte {
	b := make([]byte, size)
	rand.Read(b)
	return b
}

func BenchmarkScalePebble(b *testing.B) {
	name := "test.data" + util.RandomString(5)

	driver, err := benchtop.NewPebbleBSONDriver(name)
	if err != nil {
		b.Fatal(err)
	}
	defer driver.Close()

	columns := []benchtop.ColumnDef{{Path: "data", Type: benchtop.String}}

	table, err := driver.New(name, columns)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		inputChan := make(chan benchtop.Entry, 100)
		go func() {
			for j := 0; j < numKeys; j++ {
				key := []byte(fmt.Sprintf("key_%d", j))
				value := generateRandomBytes(valueSize)
				inputChan <- benchtop.Entry{Key: key, Value: map[string]any{"data": value}}
			}
			close(inputChan)
		}()

		err = table.Load(inputChan)
		if err != nil {
			b.Fatal(err)
		}

	}
}

func BenchmarkScaleBson(b *testing.B) {
	name := "test.data" + util.RandomString(5)

	driver, err := benchtop.NewBSONDriver(name)
	if err != nil {
		b.Fatal(err)
	}
	defer driver.Close()

	columns := []benchtop.ColumnDef{{Path: "data", Type: benchtop.String}}

	table, err := driver.New(name, columns)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		inputChan := make(chan benchtop.Entry, 100)
		go func() {
			for j := 0; j < numKeys; j++ {
				key := []byte(fmt.Sprintf("key_%d", j))
				value := generateRandomBytes(valueSize)
				inputChan <- benchtop.Entry{Key: key, Value: map[string]any{"data": value}}
			}
			close(inputChan)
		}()

		err = table.Load(inputChan)
		if err != nil {
			b.Fatal(err)
		}
	}
}
