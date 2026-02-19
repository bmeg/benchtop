package jsontable

import (
	"bytes"
	"encoding/binary"
	"strings"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
)

// Specify a table type prefix to differentiate between edge tables and vertex tables
func (dr *JSONDriver) getMaxTablePrefix() uint16 {
	// Note: Caller must hold dr.Lock

	// 1. Try to load from persistent system counter
	val, closer, err := dr.Pkv.Get(benchtop.MaxTableIDKey)
	if err == nil {
		defer closer.Close()
		if len(val) >= 2 {
			max := binary.LittleEndian.Uint16(val)
			newId := max + 1

			// Update counter
			newVal := make([]byte, 2)
			binary.LittleEndian.PutUint16(newVal, newId)
			dr.Pkv.Set(benchtop.MaxTableIDKey, newVal, nil)

			log.Debugf("Assigned new TableId %d from persistent counter", newId)
			return newId
		}
	}

	// 2. Fallback: Scan existing tables to find max (Recovery/First run)
	// Start with ID 1 to avoid sentinel issues with ID 0
	prefix := []byte{benchtop.TablePrefix}
	maxID := uint16(1)
	dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			val, err := it.Value()
			if err != nil {
				continue
			}
			var tinfo benchtop.TableInfo
			if err := sonic.ConfigFastest.Unmarshal(val, &tinfo); err == nil {
				if tinfo.TableId >= maxID {
					maxID = tinfo.TableId + 1
				}
				log.Debugf("Found existing table %s with ID %d", tinfo.Name, tinfo.TableId)
			}
		}
		return nil
	})

	// Save the found max for next time
	newVal := make([]byte, 2)
	binary.LittleEndian.PutUint16(newVal, maxID)
	dr.Pkv.Set(benchtop.MaxTableIDKey, newVal, nil)

	log.Infof("Initialized persistent TableId counter starting at %d", maxID)
	return maxID
}

func (dr *JSONDriver) addTable(Name string, TinfoMarshal []byte) error {
	log.Debugf("addTable: %s", Name)
	nkey := benchtop.NewTableKey([]byte(Name))
	err := dr.Pkv.Set(nkey, TinfoMarshal, nil)
	if err != nil {
		log.Errorf("addTable failed for %s: %v", Name, err)
	}
	return err
}

func (dr *JSONDriver) dropTable(name string) error {
	nkey := benchtop.NewTableKey([]byte(name))
	return dr.Pkv.Delete(nkey, nil)

}

func (dr *JSONDriver) getTableInfo(name string) (benchtop.TableInfo, error) {
	log.Debugf("getTableInfo: searching for %s", name)
	nkey := benchtop.NewTableKey([]byte(name))
	value, closer, err := dr.Pkv.Get(nkey)
	if err != nil {
		log.Debugf("getTableInfo: direct lookup failed for %s: %v", name, err)
		if err == pebble.ErrNotFound {
			// Fallback: Scan headers to see if we can find it
			prefix := []byte{benchtop.TablePrefix}
			var found *benchtop.TableInfo
			_ = dr.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
				for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
					val, err := it.Value()
					if err != nil {
						continue
					}
					var tinfo benchtop.TableInfo
					if err := sonic.ConfigFastest.Unmarshal(val, &tinfo); err == nil {
						if strings.EqualFold(tinfo.Name, name) {
							found = &tinfo
							return nil
						}
					}
				}
				return nil
			})
			if found != nil {
				log.Warningf("Found table %s using scan fallback, primary lookup failed", name)
				return *found, nil
			}
		}
		return benchtop.TableInfo{}, err
	}
	defer closer.Close()
	var tinfo benchtop.TableInfo
	if err := sonic.ConfigFastest.Unmarshal(value, &tinfo); err != nil {
		log.Errorf("getTableInfo: failed to unmarshal %s: %v", name, err)
		return benchtop.TableInfo{}, err
	}
	return tinfo, nil
}

func (dr *JSONDriver) AddTableEntryInfo(tx *pebblebulk.PebbleBulk, rowId []byte, rowLoc *benchtop.RowLoc) error {
	value := benchtop.EncodeRowLoc(rowLoc)
	posKey := benchtop.NewPosKey(rowLoc.TableId, rowId)
	if tx != nil {
		err := tx.Set(posKey, value, nil)
		if err != nil {
			return err
		}
	} else {
		err := dr.Pkv.Set(posKey, value, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dr *JSONDriver) GetLocFromTableKey(tableId uint16, id []byte) (loc *benchtop.RowLoc, err error) {
	val, closer, err := dr.Pkv.Get(benchtop.NewPosKey(tableId, id))
	if err != nil {
		if err != pebble.ErrNotFound {
			log.Errorln("GetLocFromTableKey Err: ", err)
		}
		return nil, err
	}
	defer closer.Close()
	return benchtop.DecodeRowLoc(val), nil
}
