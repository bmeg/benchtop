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

// Cache defines the interface for a location lookup cache.
type Cache interface {
	Get(ctx context.Context, key string) (*benchtop.RowLoc, error)
	GetBatch(ctx context.Context, keys []string) (map[string]*benchtop.RowLoc, error)
	Set(key string, value *benchtop.RowLoc) (*benchtop.RowLoc, bool)
	Invalidate(key string) (*benchtop.RowLoc, bool)
	PreloadCache() error
}

// TableLookup is a function that, given a row ID, searches all loaded
// tables and returns the matching RowLoc.
type TableLookup func(id string) (*benchtop.RowLoc, error)

// TableScanner is a function that iterates ALL row locations across all
// loaded tables, calling fn for each one. Used by PreloadCache.
type TableScanner func(fn func(id string, loc *benchtop.RowLoc)) error

type StandardCache struct {
	pageCache      *otter.Cache[string, *benchtop.RowLoc]
	pageLoader     otter.LoaderFunc[string, *benchtop.RowLoc]
	bulkPageLoader otter.BulkLoaderFunc[string, *benchtop.RowLoc]
	tableLookup    TableLookup
	tableScanner   TableScanner
}

// Get retrieves an item from the cache. If the item is not present,
// it is automatically loaded from the underlying table index.
func (ca *StandardCache) Get(ctx context.Context, key string) (*benchtop.RowLoc, error) {
	return ca.pageCache.Get(ctx, key, ca.pageLoader)
}

// GetBatch retrieves multiple items from the cache.
func (ca *StandardCache) GetBatch(ctx context.Context, keys []string) (map[string]*benchtop.RowLoc, error) {
	result := make(map[string]*benchtop.RowLoc, len(keys))
	var missing []string
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
func (ca *StandardCache) Set(key string, value *benchtop.RowLoc) (*benchtop.RowLoc, bool) {
	return ca.pageCache.Set(key, value)
}

// Invalidate removes an item from the cache.
func (ca *StandardCache) Invalidate(key string) (*benchtop.RowLoc, bool) {
	return ca.pageCache.Invalidate(key)
}

// NewStandardCache creates a Cache that uses provided lookup and scanner functions.
func NewStandardCache(lookup TableLookup, scanner TableScanner) Cache {
	c := &StandardCache{
		tableLookup:  lookup,
		tableScanner: scanner,
		pageCache: otter.Must(&otter.Options[string, *benchtop.RowLoc]{
			MaximumSize: 10_000_000,
		}),
	}
	c.pageLoader = otter.LoaderFunc[string, *benchtop.RowLoc](func(ctx context.Context, key string) (*benchtop.RowLoc, error) {
		loc, err := lookup(key)
		if err != nil {
			return &benchtop.RowLoc{}, err
		}
		return loc, nil
	})

	c.bulkPageLoader = otter.BulkLoaderFunc[string, *benchtop.RowLoc](func(ctx context.Context, keys []string) (map[string]*benchtop.RowLoc, error) {
		result := make(map[string]*benchtop.RowLoc, len(keys))
		for _, key := range keys {
			loc, err := lookup(key)
			if err != nil {
				continue
			}
			if loc != nil {
				result[key] = loc
			}
		}
		return result, nil
	})
	return c
}

// NewKVCache creates a Cache that uses a Pebble KVStore for lookups.
// This is for backward compatibility with the original JSONDriver.
func NewKVCache(kv pebblebulk.KVStore) Cache {
	lookup := TableLookup(func(id string) (*benchtop.RowLoc, error) {
		val, closer, err := kv.Get([]byte(id))
		if err != nil {
			return nil, err
		}
		defer closer.Close()
		return benchtop.DecodeRowLoc(val), nil
	})
	scanner := TableScanner(func(fn func(id string, loc *benchtop.RowLoc)) error {
		prefix := []byte{benchtop.PosPrefix}
		return kv.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
				_, id := benchtop.ParsePosKey(it.Key())
				val, err := it.Value()
				if err != nil {
					continue
				}
				loc := benchtop.DecodeRowLoc(val)
				if loc != nil {
					fn(string(id), loc)
				}
			}
			return nil
		})
	})
	return NewStandardCache(lookup, scanner)
}

// PreloadCache iterates the table scanner and populates the in-memory cache.
func (ca *StandardCache) PreloadCache() error {
	L_Start := time.Now()
	count := 0
	err := ca.tableScanner(func(id string, loc *benchtop.RowLoc) {
		if loc != nil {
			ca.pageCache.Set(id, loc)
			count++
		}
	})
	if err == nil {
		log.Debugf("Successfully preloaded %d keys in RowLoc cache in %v", count, time.Since(L_Start))
	}
	return err
}
