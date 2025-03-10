package tables

import (
	"fmt"

	"github.com/akrylysov/pogreb"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "tables <db>",
	Short: "List tables",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {

		dbPath := args[0]
		pogrebPath := args[1]
		pg, err := pogreb.Open(pogrebPath, nil)
		if err != nil {
			return err
		}

		driver, err := bsontable.NewBSONDriver(dbPath, pg)
		if err != nil {
			return err
		}

		for _, l := range driver.List() {
			fmt.Printf("%s\n", l)
		}

		return nil
	},
}
