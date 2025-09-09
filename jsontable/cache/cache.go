package cache

import (
	"bytes"
	"context"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/log"
	"github.com/maypok86/otter/v2"
)

type JSONCache struct {
	pageCache      *otter.Cache[string, *benchtop.RowLoc]
	pageLoader     otter.LoaderFunc[string, *benchtop.RowLoc]
	bulkPageLoader otter.BulkLoaderFunc[string, *benchtop.RowLoc]
	kv             pebblebulk.KVStore
}

// Get retrieves an item from the cache. If the item is not present,
// it is automatically loaded from the underlying KV store.
func (ca *JSONCache) Get(ctx context.Context, key string) (*benchtop.RowLoc, error) {
	return ca.pageCache.Get(ctx, key, ca.pageLoader)
}

// Set adds or updates an item in the cache.
func (ca *JSONCache) Set(key string, value *benchtop.RowLoc) (*benchtop.RowLoc, bool) {
	return ca.pageCache.Set(key, value)
}

// Delete removes an item from the cache.
func (ca *JSONCache) Invalidate(key string) (*benchtop.RowLoc, bool) {
	return ca.pageCache.Invalidate(key)
}

func NewJSONCache(kv pebblebulk.KVStore) *JSONCache {
	cache := &JSONCache{
		kv: kv,
		pageCache: otter.Must(&otter.Options[string, *benchtop.RowLoc]{
			MaximumSize: 10_000_000,
		}),
	}
	cache.pageLoader = otter.LoaderFunc[string, *benchtop.RowLoc](func(ctx context.Context, key string) (*benchtop.RowLoc, error) {
		log.Debugln("Cache miss, loading from kv: ", key)
		val, closer, err := kv.Get([]byte(key))
		if err != nil {
			if err.Error() != "pebble: not found" { // Handle Pebble-specific error generically
				log.Errorf("Err on kv.Get for key %s in CacheLoader: %v", key, err)
			}
			return &benchtop.RowLoc{}, err
		}
		closer.Close()
		return benchtop.DecodeRowLoc(val), nil
	})

	cache.bulkPageLoader = otter.BulkLoaderFunc[string, *benchtop.RowLoc](func(ctx context.Context, keys []string) (map[string]*benchtop.RowLoc, error) {
		prefix := []byte{benchtop.PosPrefix}
		result := make(map[string]*benchtop.RowLoc, len(keys))
		err := kv.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				_, id := benchtop.ParsePosKey(it.Key())
				val, err := it.Value()
				if err != nil {
					log.Errorf("Err on it.Value() in bulkLoader: %v", err)
					continue
				}
				loc := benchtop.DecodeRowLoc(val)
				result[string(id)] = loc
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return result, nil
	})
	return cache
}

func (ca *JSONCache) PreloadCache() error {
	var keys []string
	prefix := []byte{benchtop.PosPrefix}
	L_Start := time.Now()
	err := ca.kv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			_, id := benchtop.ParsePosKey(it.Key())
			keys = append(keys, string(id))
		}
		return nil
	})
	if err != nil {
		return err
	}
	_, err = ca.pageCache.BulkGet(context.Background(), keys, ca.bulkPageLoader)
	if err == nil {
		log.Debugf("Successfully loaded %d keys in RowLoc cache in %s", len(keys), time.Now().Sub(L_Start).String())
	}
	return err
}
