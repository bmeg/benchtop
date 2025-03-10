package get

import (
	"encoding/json"
	"fmt"

	"github.com/akrylysov/pogreb"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "get <db> <table> <key>",
	Short: "List keys",
	Long:  ``,
	Args:  cobra.MinimumNArgs(3),
	RunE: func(cmd *cobra.Command, args []string) error {

		dbPath := args[0]
		tableName := args[1]
		keys := args[2:]

		pogrebPath := args[2]
		pg, err := pogreb.Open(pogrebPath, nil)
		if err != nil {
			return err
		}


		driver, err := bsontable.NewBSONDriver(dbPath, pg)
		if err != nil {
			return err
		}

		table, err := driver.Get(tableName)
		if err != nil {
			return err
		}

		for _, key := range keys {
			data, err := table.GetRow([]byte(key))
			if err == nil {
				out, err := json.Marshal(data)
				if err != nil {
					return err
				}
				fmt.Printf("%s\n", out)
			}
		}
		return nil
	},
}
