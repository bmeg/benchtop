package keys

import (
	"fmt"

	"github.com/bmeg/benchtop/jsontable"
	jTable "github.com/bmeg/benchtop/jsontable/table"

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

		driver, err := jsontable.NewJSONDriver(dbPath)
		if err != nil {
			return err
		}

		table, err := driver.Get(tableName)
		if err != nil {
			return err
		}

		jT, _ := table.(*jTable.JSONTable)

		keys, err := driver.ListTableKeys(jT.TableId)
		if err != nil {
			return err
		}
		for k := range keys {
			fmt.Printf("%s\n", k.Key)
		}
		return nil
	},
}
