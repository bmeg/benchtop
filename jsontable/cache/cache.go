package cache

import (
	"bytes"
	"context"
	"fmt"
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

// GetBatch retrieves multiple items from the cache.
func (ca *JSONCache) GetBatch(ctx context.Context, keys []string) (map[string]*benchtop.RowLoc, error) {
	result := make(map[string]*benchtop.RowLoc, len(keys))
	var missing []string
	// Dummy loader that just returns an error so we can detect cache misses
	// without triggering a real (sequential) load.
	dummyLoader := otter.LoaderFunc[string, *benchtop.RowLoc](func(ctx context.Context, key string) (*benchtop.RowLoc, error) {
		return nil, fmt.Errorf("miss")
	})

	for _, k := range keys {
		if loc, err := ca.pageCache.Get(ctx, k, dummyLoader); err == nil {
			result[k] = loc
		} else {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		missed, err := ca.bulkPageLoader(ctx, missing)
		if err != nil {
			return result, err
		}
		for k, loc := range missed {
			ca.pageCache.Set(k, loc)
			result[k] = loc
		}
	}
	return result, nil
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
		result := make(map[string]*benchtop.RowLoc, len(keys))
		// Iterate over specific keys to load from KV
		for _, key := range keys {
			val, closer, err := kv.Get([]byte(key))
			if err != nil {
				if err.Error() != "pebble: not found" {
					log.Errorf("Err on kv.Get for key %s in bulkLoader: %v", key, err)
				}
				continue
			}
			loc := benchtop.DecodeRowLoc(val)
			if loc != nil && loc.Size > 0 {
				result[key] = loc
			}
			closer.Close()
		}
		return result, nil
	})
	return cache
}

func (ca *JSONCache) PreloadCache() error {
	prefix := []byte{benchtop.PosPrefix}
	L_Start := time.Now()

	count := 0
	err := ca.kv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			_, id := benchtop.ParsePosKey(it.Key())
			rowId := string(id)

			val, err := it.Value()
			if err != nil {
				log.Errorf("PreloadCache: error reading value for key %s: %v", rowId, err)
				continue
			}

			loc := benchtop.DecodeRowLoc(val)
			if loc == nil || loc.Size == 0 {
				// Skip invalid/zero locations
				continue
			}

			ca.pageCache.Set(rowId, loc)
			count++
		}
		return nil
	})

	if err == nil {
		log.Debugf("Successfully preloaded %d keys in RowLoc cache in %v", count, time.Since(L_Start))
	}
	return err
}
