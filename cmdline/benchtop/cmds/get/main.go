package get

import (
	"encoding/json"
	"fmt"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/grip/log"
	"github.com/cockroachdb/pebble"
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

		driver, err := bsontable.NewBSONDriver(dbPath)
		if err != nil {
			return err
		}

		table, err := driver.Get(tableName)
		if err != nil {
			return err
		}

		TS, _ := driver.(*bsontable.BSONDriver)
		for _, key := range keys {
			val, closer, err := TS.Pb.Db.Get([]byte(key))
			if err != nil {
				if err != pebble.ErrNotFound {
					log.Errorf("Err on dr.Pb.Get for key %s in CacheLoader: %v", key, err)
				}
				log.Errorln("ERR: ", err)
			}
			fmt.Println("VAL: ", val)
			offset, size := benchtop.ParsePosValue(val)
			closer.Close()

			data, err := table.GetRow(benchtop.RowLoc{Offset: offset, Size: size})
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
