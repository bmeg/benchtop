package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable"
	"github.com/bmeg/benchtop/util"

	"github.com/schollz/progressbar/v3"
)

func main() {
	flag.Parse()

	file := flag.Arg(0)
	dbPath := flag.Arg(1)

	db, err := jsontable.NewJSONDriver(dbPath)
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}

	table, err := db.New("peptides", []benchtop.ColumnDef{})
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}

	lineCount, _ := util.LineCounter(file)

	lines, err := util.StreamLines(file, 10)
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}
	records := make(chan benchtop.Row, 10)

	go func() {
		defer close(records)
		bar := progressbar.Default(int64(lineCount))

		for l := range lines {
			row := strings.Split(l, "\t")

			//data := map[string]any{}
			//json.Unmarshal([]byte(row[1]), &data)
			data := []any{}
			json.Unmarshal([]byte(row[1]), &data)
			entry := map[string]any{
				"embedding": data,
			}
			records <- benchtop.Row{Id: []byte(row[0]), Data: entry}
			bar.Add(1)
		}
	}()
	table.Load(records)
	db.Close()
}
