package load

import (
	"encoding/json"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/util"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

var keyField = "key"

var Cmd = &cobra.Command{
	Use:   "load <db> <table> <filepath>",
	Short: "Load data",
	Long:  ``,
	Args:  cobra.ExactArgs(3),
	RunE: func(cmd *cobra.Command, args []string) error {

		dbPath := args[0]
		tableName := args[1]
		filePath := args[2]

		driver, err := benchtop.NewBSONDriver(dbPath)
		if err != nil {
			return err
		}

		table, err := driver.New(tableName, []benchtop.ColumnDef{})
		if err != nil {
			return err
		}

		lineCount, _ := util.LineCounter(filePath)

		lines, err := util.StreamLines(filePath, 10)
		if err != nil {
			return err
		}

		records := make(chan benchtop.Entry, 10)
		go func() {
			defer close(records)
			bar := progressbar.Default(int64(lineCount))

			for l := range lines {
				data := map[string]any{}
				json.Unmarshal([]byte(l), &data)

				if key, ok := data[keyField]; ok {
					keyStr := key.(string)
					records <- benchtop.Entry{Key: []byte(keyStr), Value: data}
				}
				bar.Add(1)
			}
		}()
		table.Load(records)
		driver.Close()
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVarP(&keyField, "key", "k", keyField, "Field to use for key")
}
