package tables

import (
	"fmt"

	"github.com/bmeg/benchtop/jsontable"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "tables <db>",
	Short: "List tables",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {

		dbPath := args[0]

		driver, err := jsontable.NewJSONDriver(dbPath)
		if err != nil {
			return err
		}

		for _, l := range driver.List() {
			fmt.Printf("%s\n", l)
		}

		return nil
	},
}
