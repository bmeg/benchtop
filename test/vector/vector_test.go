package test

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/cockroachdb/pebble"
)

// GenerateBioVectors (unchanged)
func genBioVectors(numVectors, dim, numClusters int) map[uint64][]float32 {
	vmap := make(map[uint64][]float32, numVectors)
	rand.Seed(time.Now().UnixNano())

	centers := make([][]float32, numClusters)
	for c := 0; c < numClusters; c++ {
		center := make([]float32, dim)
		for j := 0; j < dim; j++ {
			center[j] = rand.Float32() * 100
		}
		centers[c] = center
	}

	for i := 0; i < numVectors; i++ {
		c := make([]float32, dim)
		cluster := rand.Intn(numClusters)
		for j := 0; j < dim; j++ {
			noise := float32(rand.NormFloat64()) * 0.5
			c[j] = centers[cluster][j] + noise
		}
		vmap[uint64(i)] = c
	}
	return vmap
}

func BenchmarkBenchtopHNSW(b *testing.B) {
	numVectors := 10000
	dim := 200
	numClusters := 10
	K := 10

	vmap := genBioVectors(numVectors, dim, numClusters)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rootPath := filepath.Join(fmt.Sprintf("benchtop_hnsw_%d", i))
		if err := os.MkdirAll(rootPath, 0755); err != nil {
			b.Fatalf("failed to create directory: %v", err)
		}

		driver, err := bsontable.NewBSONDriver(rootPath)
		if err != nil {
			b.Fatalf("failed to create BSON driver: %v", err)
		}
		defer driver.Close()

		columns := []benchtop.ColumnDef{
			{Key: "vector", Type: benchtop.VectorArray},
		}
		table, err := driver.New("vectors", columns)
		if err != nil {
			b.Fatalf("failed to create table: %v", err)
		}

		// Insert vectors
		rows := make(chan benchtop.Row, 100)
		go func() {
			defer close(rows)
			for id, vec := range vmap {
				key := make([]byte, 8)
				binary.LittleEndian.PutUint64(key, id)
				rows <- benchtop.Row{
					Id:        key,
					TableName: "vectors",
					Data:      map[string]any{"vector": vec},
				}
			}
		}()
		start := time.Now()
		if err := table.Load(rows); err != nil {
			b.Fatalf("failed to load vectors: %v", err)
		}
		insertTime := time.Since(start)
		b.Logf("Inserted %d vectors in %v (%.2f vectors/sec)", len(vmap), insertTime, float64(len(vmap))/insertTime.Seconds())

		// Test queries
		numQueries := 10
		recallSum := 0.0
		bsonTable := table.(*bsontable.BSONTable)

		for q := 0; q < numQueries; q++ {
			qID := rand.Int63n(int64(numVectors))
			qVec := vmap[uint64(qID)]

			// Brute-force ground truth
			type neighbor struct {
				id   uint64
				dist float32
			}
			neighbors := make([]neighbor, 0, numVectors)
			bruteStart := time.Now()
			for id, vec := range vmap {
				var sum float32
				for j := range vec {
					diff := vec[j] - qVec[j]
					sum += diff * diff
				}
				neighbors = append(neighbors, neighbor{id: id, dist: sum})
			}
			sort.Slice(neighbors, func(i, j int) bool {
				return neighbors[i].dist < neighbors[j].dist
			})
			trueNeighbors := make([]uint64, K)
			for i := 0; i < K && i < len(neighbors); i++ {
				trueNeighbors[i] = neighbors[i].id
			}
			bruteTime := time.Since(bruteStart)

			// HNSW Search
			hnswStart := time.Now()
			resultsChan, err := bsonTable.VectorSearch("vector", qVec, K)
			if err != nil {
				b.Fatalf("vector search failed: %v", err)
			}
			results := make([]benchtop.VectorSearchResult, 0, K)
			for _, r := range resultsChan {
				results = append(results, r)
			}
			hnswTime := time.Since(hnswStart)

			// Compute recall
			hits := 0
			for _, result := range results {
				id := binary.LittleEndian.Uint64(result.Key)
				for _, trueID := range trueNeighbors {
					if id == trueID {
						hits++
						break
					}
				}
			}
			recall := float64(hits) / float64(K)
			recallSum += recall

			speedup := float64(bruteTime) / float64(hnswTime)
			b.Logf("Query %d: HNSW found %d/%d true neighbors, Recall=%.3f, HNSW time=%v, Brute time=%v, Speedup=%.2fx",
				q, hits, K, recall, hnswTime, bruteTime, speedup)
		}

		avgRecall := recallSum / float64(numQueries)
		b.Logf("Average Recall across %d queries: %.3f", numQueries, avgRecall)
	}
}

func BenchmarkDeleteHNSW(b *testing.B) {
	numVectors := 10000
	dim := 200
	numClusters := 10
	vmap := genBioVectors(numVectors, dim, numClusters)

	rootPath := filepath.Join(fmt.Sprintf("benchtop_hnsw_delete"))
	if err := os.MkdirAll(rootPath, 0755); err != nil {
		b.Fatalf("failed to create directory: %v", err)
	}

	driver, err := bsontable.NewBSONDriver(rootPath)
	if err != nil {
		b.Fatalf("failed to create BSON driver: %v", err)
	}
	defer driver.Close()

	columns := []benchtop.ColumnDef{
		{Key: "vector", Type: benchtop.VectorArray},
	}
	table, err := driver.New("vectors", columns)
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	b.ResetTimer()
	// Insert vectors
	rows := make(chan benchtop.Row, 100)
	go func() {
		defer close(rows)
		for id, vec := range vmap {
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
		b.Fatalf("failed to load vectors: %v", err)
	}

	// Test deletion
	b.Run("DeleteRow", func(b *testing.B) {
		bsonTable := table.(*bsontable.BSONTable)
		for i := 0; i < b.N; i++ {
			id := uint64(rand.Intn(numVectors))
			key := make([]byte, 8)
			binary.LittleEndian.PutUint64(key, id)
			err := bsonTable.DeleteRow(key)
			if err != nil {
				b.Fatalf("failed to delete row %d: %v", id, err)
			}

			// should return not found here
			_, err = bsonTable.GetRow(key)
			if err != pebble.ErrNotFound {
				b.Error(err)
			}

			// Verify deletion from HNSW (optional)
			resultsChan, err := bsonTable.VectorSearch("vector", vmap[id], 1)
			if err != nil {
				b.Fatalf("vector search failed post-deletion: %v", err)
			}
			found := false
			for _, r := range resultsChan {
				if binary.LittleEndian.Uint64(r.Key) == id {
					found = true
					break
				}
			}
			if found {
				b.Errorf("vector for ID %d still exists in HNSW after deletion", id)
			}
		}
	})

	b.Run("GetRow", func(b *testing.B) {
		bsonTable := table.(*bsontable.BSONTable)
		for i := 0; i < b.N; i++ {
			id := uint64(rand.Intn(numVectors))
			key := make([]byte, 8)
			binary.LittleEndian.PutUint64(key, id)
			row, err := bsonTable.GetRow(key)
			if err != nil {
				b.Error(err)
			}
			b.Log("ROW: ", row)
		}
	})
	os.RemoveAll(rootPath)

}
