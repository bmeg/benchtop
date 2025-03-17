package keys

import (
	"fmt"

	"github.com/bmeg/benchtop/bsontable"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "keys <db> <table>",
	Short: "List keys",
	Long:  ``,
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {

		dbPath := args[0]
		tableName := args[1]

		driver, err := bsontable.NewBSONDriver(dbPath)
		if err != nil {
			return err
		}

		table, err := driver.Get(tableName)
		if err != nil {
			return err
		}

		keys, err := table.Keys()
		if err != nil {
			return err
		}
		for k := range keys {
			fmt.Printf("%s\n", k)
		}
		return nil
	},
}
