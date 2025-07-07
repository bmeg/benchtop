package bsontable

import (
	"bytes"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
)

func (dr *BSONDriver) PreloadCache() error {
	err := dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for _, table := range dr.Tables {
			prefix := benchtop.NewPosKeyPrefix(table.TableId)
			count := 0
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				tableId, id := benchtop.ParsePosKey(it.Key())
				val, err := it.Value()
				if err != nil {
					log.Errorf("Err on it.Value() in PreloadCache")
				}
				offset, size := benchtop.ParsePosValue(val)
				dr.PageCache.Set(string(id)[2:], benchtop.RowLoc{Offset: offset, Size: size, Label: tableId})
				count++
			}
			log.Debugf("Finished loading %s entries on table: %d", table.Name, count)
		}
		return nil
	})
	return err
}
