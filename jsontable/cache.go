package jsontable

import (
	"bytes"
	"context"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
)

func (dr *JSONDriver) PreloadCache() error {
	var keys []string
	prefix := []byte{benchtop.PosPrefix}
	L_Start := time.Now()
	err := dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			_, id := benchtop.ParsePosKey(it.Key())
			keys = append(keys, string(id))
		}
		return nil
	})
	if err != nil {
		return err
	}
	_, err = dr.PageCache.BulkGet(context.Background(), keys, dr.BulkPageLoader)
	if err == nil {
		log.Debugf("Successfully loaded %d keys in RowLoc cache in %s", len(keys), (time.Now().Sub(L_Start).String()))
	}
	return err
}
