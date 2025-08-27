package jsontable

import (
	"bytes"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
)

// Specify a table type prefix to differentiate between edge tables and vertex tables
func (dr *JSONDriver) getMaxTablePrefix() uint16 {
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

func (dr *JSONDriver) addTable(Name string, TinfoMarshal []byte) error {
	nkey := benchtop.NewTableKey([]byte(Name))
	return dr.db.Set(nkey, TinfoMarshal, nil)
}

func (dr *JSONDriver) dropTable(name string) error {
	nkey := benchtop.NewTableKey([]byte(name))
	return dr.db.Delete(nkey, nil)

}

func (dr *JSONDriver) getTableInfo(name string) (benchtop.TableInfo, error) {
	value, closer, err := dr.db.Get([]byte(name))
	if err != nil {
		return benchtop.TableInfo{}, err
	}
	tinfo := benchtop.TableInfo{}
	sonic.ConfigFastest.Unmarshal(value, &tinfo)
	closer.Close()
	return tinfo, nil
}
