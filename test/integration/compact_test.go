package test

import (
	"os"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable"
	"github.com/bmeg/benchtop/test/fixtures"
	"github.com/bmeg/benchtop/util"
)

func TestCompact(t *testing.T) {
	dbname := "test_compact.data" + util.RandomString(5)
	defer os.RemoveAll(dbname)

	dr, err := jsontable.NewJSONDriver(dbname)
	if err != nil {
		t.Fatal(err)
	}

	ts, err := dr.New("table_1", []benchtop.ColumnDef{
		{Key: "field1"},
		{Key: "name"},
	})
	if err != nil {
		t.Fatal(err)
	}

	bT, _ := ts.(*jsontable.JSONTable)
	for k, r := range fixtures.ScanData {
		loc, err := bT.AddRow(benchtop.Row{Id: []byte(k), TableName: "table_1", Data: r})
		if err != nil {
			t.Fatal(err)
		}
		err = bT.AddTableEntryInfo(nil, []byte(k), *loc)

	}

	offset, size, err := bT.GetBlockPos([]byte("key4"))
	if err != nil {
		t.Error(err)
	}
	err = ts.DeleteRow(benchtop.RowLoc{Offset: offset, Size: size, Label: bT.TableId}, []byte("key4"))
	if err != nil {
		t.Fatal(err)
	}

	/*
		 Compact is not working and not used in grip currently but probably should be in the near future, next PRs

				// Get the file size before compaction
				table, err := dr.Get("table_1")
				if err != nil {
					t.Fatal(err)
				}

					beforeStat, err := os.Stat(dbname + "/TABLES/" + table.(*jsontable.BSONTable).FileName)
					if err != nil {
						t.Fatal(err)
					}
					//beforeSize := beforeStat.Size()

					err = ts.Compact()
					if err != nil {
						t.Fatal(err)
						}

					afterStat, err := os.Stat(dbname + "/TABLES/" + table.(*bsontable.BSONTable).FileName)
					if err != nil {
						t.Fatal(err)
					}
					afterSize := afterStat.Size()

					if afterSize >= beforeSize {
						t.Errorf("Expected file size to decrease after compaction, but it remained the same or increased: before=%d, after=%d", beforeSize, afterSize)
					} else {
						t.Logf("size before=%d, after=%d", beforeSize, afterSize)
					}

					testChan := ts.Scan(true, nil)
					if err != nil {
						t.Error(err)
					}

					t.Log("elems after")
					for elem := range testChan {
						t.Log(elem)
					}

					pKey := benchtop.NewPosKey(uint16(0), []byte("key8"))
					val, closer, err := bT.Pb.Db.Get(pKey)
					if err != nil {
						if err != pebble.ErrNotFound {
							log.Errorf("Err on dr.Pb.Get for key %s in CacheLoader: %v", pKey, err)
						}
						log.Errorln("ERR: ", err)
					}
					offset, size := benchtop.ParsePosValue(val)
					closer.Close()

					gotRow, err := bT.GetRow(benchtop.RowLoc{Offset: offset, Size: size, Label: 0})
					if err != nil {
						t.Error(err)
					}
					t.Log("Get key8: ", gotRow)

					if gotRow["name"] != "mnbv" {
						t.Errorf("fetched key8 but got name val %s instead", gotRow["name"])
					}

					pKey = benchtop.NewPosKey(uint16(0), []byte("key8"))
					val, closer, err = bT.Pb.Db.Get(pKey)
					if err != nil {
						if err != pebble.ErrNotFound {
							log.Errorf("Err on dr.Pb.Get for key %s in CacheLoader: %v", pKey, err)
						}
						log.Errorln("ERR: ", err)
					}
					offset, size = benchtop.ParsePosValue(val)
					closer.Close()

					// Get another key to double check that it works
					gotRow, err = bT.GetRow(benchtop.RowLoc{Offset: offset, Size: size, Label: 0})
					if err != nil {
						t.Error(err)
					}
					t.Log("Get key7: ", val)

					if gotRow["name"] != "zxcv" {
						t.Errorf("fetched key7 but got name val %s instead", gotRow["name"])
					}

					ts.Compact()
					defer dr.Close()
	*/
}
