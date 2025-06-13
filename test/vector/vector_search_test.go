package test

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/benchtop/distqueue"
)

// RandomString generates a random string of length n.
func RandomString(n int) string {
	rand.NewSource(int64(time.Now().UnixNano()))
	var letter = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letter[rand.Intn(len(letter))]
	}
	return string(b)
}

const (
	DIM   = 128
	COUNT = 1000
)

func TestInsert(t *testing.T) {

	dbname := "test_index." + RandomString(5)

	driver, err := bsontable.NewBSONDriver(dbname)

	if err != nil {
		t.Error(err)
	}

	table, err := driver.New("VECTORS", []benchtop.ColumnDef{{Key: "embedding", Type: benchtop.VectorArray}})
	if err != nil {
		t.Error(err)
	}

	vmap := map[string][]float32{}
	for i := 0; i < 100; i++ {
		c := make([]float32, DIM)
		for j := 0; j < DIM; j++ {
			c[j] = rand.Float32()
		}
		vmap[fmt.Sprintf("%d", i)] = c
	}

	loadChan := make(chan benchtop.Row)
	go func() {
		for k, v := range vmap {
			//fmt.Printf("==vector==:%s\n", k)
			loadChan <- benchtop.Row{Id: []byte(k), Data: map[string]any{"embedding": v}}
		}
		close(loadChan)
	}()
	table.Load(loadChan)

	//TODO Add search here

	qName := "10"
	qVec := vmap[qName]
	testDists := distqueue.NewMin[float32, string]()

	for k, v := range vmap {
		d := distqueue.Euclidean(v, qVec)
		testDists.Insert(d, k)
	}

	//TODO: Make this work
	/*
		out, err := table.Search("VECTORS", vmap[qName], 10)
		if err != nil {
			t.Error(err)
		}

		for _, i := range out {
			fmt.Printf("search: out: %s\n", i)
		}

		for i := 0; i < 10; i++ {
			fmt.Printf("scan out: %s\n", testDists[i].Value)
		}
	*/

	driver.Close()
	os.RemoveAll(dbname)
}
