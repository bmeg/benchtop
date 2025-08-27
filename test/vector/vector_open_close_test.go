package test

import (
	"math/rand"
)

func GenerateRandomFloat32Vectors(numVectors, dim int) map[uint64][]float32 {
	vmap := make(map[uint64][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vector := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vector[j] = rand.Float32() * 100
		}
		vmap[uint64(i)] = vector
	}
	return vmap
}

/* Not sure where this  HnswIndex.ContainsDoc( is even coming from. Not going to attempt to maintain something that I don't remember
func TestBenchtopHNSW(t *testing.T) {

	numVectors := 100
	dim := 150

	rootPath := filepath.Join(fmt.Sprintf("benchtop_hnsw_0"))
	defer os.RemoveAll(rootPath)

	if err := os.MkdirAll(rootPath, 0755); err != nil {
		t.Fatalf("failed to create directory: %v", err)
	}

	driver, err := bsontable.NewBSONDriver(rootPath)
	if err != nil {
		t.Fatalf("failed to create BSON driver: %v", err)
	}
	defer driver.Close()

	columns := []benchtop.ColumnDef{
		{Key: "vector", Type: benchtop.VectorArray},
	}
	table, err := driver.New("vectors", columns)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert vectors
	rows := make(chan benchtop.Row, 100)
	vecs := GenerateRandomFloat32Vectors(numVectors, dim)
	go func() {
		defer close(rows)
		for id, vec := range vecs {
			key := make([]byte, 8)
			binary.LittleEndian.PutUint64(key, id)
			rows <- benchtop.Row{
				Id:        key,
				TableName: "vectors",
				Data:      map[string]any{"vector": vec},
			}
		}
	}()
	if err := table.Load(rows); err != nil {
		t.Fatalf("failed to load vectors: %v", err)
	}

	val := table.(*bsontable.BSONTable).HnswIndex.ContainsDoc(uint64(rand.Int63n(int64(numVectors))))
	t.Log("VAL 1: ", val)

	driver.Close()
	or, err := bsontable.LoadBSONDriver(rootPath, "benchtop_hnsw_0")
	ot, err := or.Get("vectors")
	if err != nil {
		t.Error(err)
	}

	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, uint64(rand.Int63n(int64(numVectors))))

		row, err := ot.GetRow(key)
		t.Log("ROW: ", row)
		if err != nil {
			t.Error(err)
			}

	val = ot.(*bsontable.BSONTable).HnswIndex.ContainsDoc(uint64(rand.Int63n(int64(numVectors))))
	t.Log("VAL 2: ", val)

	results, err := ot.VectorSearch("vector", vecs[uint64(rand.Int63n(int64(numVectors)))], 10)
	if err != nil {
		t.Fatalf("vector search failed: %v", err)
	}

	t.Log("RESULTS: ", results)

	or.Close()
}

func TestPersistence(t *testing.T) {
	rootPath := "test_hnsw"
	os.RemoveAll(rootPath)
	driver, err := bsontable.NewBSONDriver(rootPath)
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}
	table, err := driver.New("vectors", []benchtop.ColumnDef{{Key: "vector", Type: benchtop.VectorArray}})
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	id := uint64(1)
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, id)
	vec := []float32{1.0, 2.0, 3.0}
	table.AddRow(benchtop.Row{Id: key, TableName: "vectors", Data: map[string]any{"vector": vec}})
	//fmt.Printf("TABLE 1B: %#v\n", table.(*bsontable.BSONTable).HnswIndex)
	//fmt.Printf("TABLE 1C: %#v\n", table.(*bsontable.BSONTable).Store)

	val := table.(*bsontable.BSONTable).HnswIndex.ContainsDoc(uint64(1))
	t.Log("VAL: ", val)

	driver.Close()

	// Reopen
	driver, err = bsontable.LoadBSONDriver(rootPath)
	if err != nil {
		t.Fatalf("failed to load driver: %v", err)
	}

	table, err = driver.Get("vectors")

	//fmt.Printf("TABLE 2B: %#v\n", table.(*bsontable.BSONTable).HnswIndex)
	//fmt.Printf("TABLE 2C: %#v\n", table.(*bsontable.BSONTable).Store)

	bsonTable := table.(*bsontable.BSONTable)
	twoval := bsonTable.HnswIndex.ContainsDoc(uint64(1))
	t.Log("TWOVAL: ", twoval)
	driver.Close()

}
*/
