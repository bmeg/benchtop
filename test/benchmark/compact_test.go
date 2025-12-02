package test

const (
	numKeys       = 1000
	valueSize     = 5024
	NumDeleteKeys = 200
)

/*  Compact not implemented currently
func BenchmarkCompactJson(b *testing.B) {
	var compactjsoname = "test.json" + util.RandomString(5)
	defer os.RemoveAll(compactjsoname)

	b.Log("BenchmarkScaleWriteJson start")

	compactjsonDriver, err := jsontable.NewJSONDriver(compactjsoname)
	if err != nil {
		b.Fatal(err)
	}

	columns := []benchtop.ColumnDef{{Key: "data"}}

	compactjsonTable, err := compactjsonDriver.New(compactjsoname, columns)
	if err != nil {
		b.Fatal(err)
	}

	inputChan := make(chan *benchtop.Row, 100)
	go func() {
		count := 0
		for j := 0; j < numKeys; j++ {
			key := []byte(fmt.Sprintf("key_%d", j))
			value := fixtures.GenerateRandomBytes(valueSize)
			inputChan <- &benchtop.Row{Id: key, Data: map[string]any{"data": value}}
			count++
		}
		b.Logf("Inserted %d entries into inputChan", count)
		close(inputChan)
	}()

	//func (dr *JSONDriver) BulkLoad(inputs chan *benchtop.Row, tx *pebblebulk.PebbleBulk) error {

	jsonDriver, ok := compactjsonDriver.(*jsontable.JSONDriver)
	if !ok {
		b.Fatalf("invalid table type for %s", compactjsoname)
	}

	jT, ok := compactjsonTable.(*jTable.JSONTable)
	if !ok {
		b.Fatalf("invalid table type for %s", compactjsoname)
	}

	b.Log("start load")
	if err := jsonDriver.BulkLoad(inputChan, nil); err != nil {
		b.Fatal(err)
	}
	b.Log("Load completed successfully")

	jT, _ = compactjsonTable.(*jTable.JSONTable)

	keys, err := compactjsonTable.Keys()
	if err != nil {
		b.Fatal(err)
	}

	randomIndexSet, err := fixtures.GetRandomUniqueIntegers(NumDeleteKeys, numKeys)
	if err != nil {
		b.Fatal(err)
	}

	count := 0
	deleted := 0
	for key := range keys {
		if _, exists := randomIndexSet[count]; exists {
			loc, err := jT.GetBlockPos(key.Key)
			if err != nil {
				b.Error(err)
			}
			if err := compactjsonTable.DeleteRow(loc, key.Key); err != nil {
				b.Fatal(err)
			}
			deleted++
		}
		count++
	}
	b.Logf("Deleted %d keys", deleted)

	b.Log("start compact")
	b.ResetTimer()

	if err := compactjsonTable.Compact(); err != nil {
		b.Fatal(err)
	}

	keysAfterCompact, err := compactjsonTable.Keys()
	if err != nil {
		b.Fatal(err)
	}

	keyCount := 0
	for _ = range keysAfterCompact {
		keyCount++
	}
	if keyCount != (numKeys - NumDeleteKeys) {
		b.Fatalf("Keycount %d not equal expected %d", keyCount, (numKeys - NumDeleteKeys))
	}

	b.Logf("Keys after compaction: %d", keyCount)
}
*/
