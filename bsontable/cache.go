package bsontable

import (
	"bytes"
	"time"
	"context"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
	"github.com/maypok86/otter/v2"
)


func (dr *BSONDriver) PreloadCache() error {
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

    bulkLoader := otter.BulkLoaderFunc[string, benchtop.RowLoc](func(ctx context.Context, keys []string) (map[string]benchtop.RowLoc, error) {
        result := make(map[string]benchtop.RowLoc, len(keys))
        err := dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
            for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
                tableId, id := benchtop.ParsePosKey(it.Key())
                val, err := it.Value()
                if err != nil {
                    log.Errorf("Err on it.Value() in bulkLoader: %v", err)
                    continue
                }
                offset, size := benchtop.ParsePosValue(val)
                result[string(id)] = benchtop.RowLoc{Offset: offset, Size: size, Label: tableId}
            
            }
            return nil
        })
        if err != nil {
            return nil, err
        }
        return result, nil
    })

    _, err = dr.PageCache.BulkGet(context.Background(), keys, bulkLoader)
   	if err == nil {
		log.Debugf("Successfully loaded %d keys in RowLoc cache in %s", len(keys), (time.Now().Sub(L_Start).String()))
	}
    return err
}


/*
 * Old slow Cache Loading function. Will keep this here until it is clear that new cache loading function works as expected.
 func (dr *BSONDriver) PreloadCache() error {
	L_Start := time.Now()
	err := dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		prefix := []byte{benchtop.PosPrefix}
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			tableId, id := benchtop.ParsePosKey(it.Key())
			val, err := it.Value()
			if err != nil {
				log.Errorf("Err on it.Value() in PreloadCache")
			}
			offset, size := benchtop.ParsePosValue(val)
			dr.PageCache.Set(string(id), benchtop.RowLoc{Offset: offset, Size: size, Label: tableId})
		}
		return nil
	})
	if err == nil {
		log.Debugf("Successfully loaded RowLoc cache in %d seconds", (time.Now().Second() - L_Start.Second()))
	}
	return err
}*/
