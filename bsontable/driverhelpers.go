package bsontable

import (
	"bytes"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
	"go.mongodb.org/mongo-driver/bson"
)

// Specify a table type prefix to differentiate between edge tables and vertex tables
func (dr *BSONDriver) getMaxTablePrefix() uint16 {
	// get the max table uint32. Useful for fetching keys.
	prefix := []byte{benchtop.TablePrefix}
	
	maxID := uint16(0)
	dr.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			// fishing for edge cases
			if maxID == ^uint16(0) {
				log.Errorf("getMaxTablePrefix( maxID exceeds uint16 max value")
			}
			maxID++
		}
		return nil
	})	
	
	return maxID
}

func (dr *BSONDriver) addTable(Name string, TinfoMarshal []byte) error {
	nkey := benchtop.NewTableKey([]byte(Name))
	return dr.db.Set(nkey, TinfoMarshal, nil)
}

func (dr *BSONDriver) dropTable(name string) error {
	nkey := benchtop.NewTableKey([]byte(name))
	return dr.db.Delete(nkey, nil)

}

func (dr *BSONDriver) getTableInfo(name string) (benchtop.TableInfo, error) {
	value, closer, err := dr.db.Get([]byte(name))
	if err != nil {
		return benchtop.TableInfo{}, err
	}
	tinfo := benchtop.TableInfo{}
	bson.Unmarshal(value, &tinfo)
	closer.Close()
	return tinfo, nil
}
