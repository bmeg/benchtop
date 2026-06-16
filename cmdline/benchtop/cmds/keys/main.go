package keys

import (
	"fmt"

	"github.com/bmeg/benchtop/jsontable"

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

		tid, err := driver.LookupTableID(tableName)
		if err != nil {
			return err
		}

		// ListTableKeys is not part of TableDriver interface, need to cast
		jd, ok := driver.(*jsontable.JSONDriver)
		if !ok {
			return fmt.Errorf("driver is not a JSONDriver")
		}

		keys, err := jd.ListTableKeys(tid)
		if err != nil {
			return err
		}
		for k := range keys {
			fmt.Printf("%s\n", k.Key)
		}
		return nil
	},
}
