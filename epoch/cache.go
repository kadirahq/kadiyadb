package epoch

import (
	"math"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
)

const (
	// ExpireAll has the maximum possible value for the int64 type.
	// Passing this for the expire function will expire all epochs.
	ExpireAll = math.MaxInt64
)

// item structs are used as items in caches to store epochs and their weights.
// Items with a higher number for weights are considered important so they less
// likely to be removed when the cache runs out of space.
type item struct {
	weight int64
	epoch  *Epoch
}

// Cache is an LRU cache for epochs. The cache contains both read-only epochs
// and read-write epochs. An epoch can only be in one of these categories.
// The cache has separate limits for the number of read-only/read-write epochs.
type Cache struct {
	rosize int64
	rodata map[int64]*item
	rwsize int64
	rwdata map[int64]*item
	dbpath string
	nextID int64
	mapmtx *sync.RWMutex
	rsize  int64
}

// NewCache crates an LRU cache with given RO/RW size limits
func NewCache(rwsz, rosz int64, dir string, rsz int64) (c *Cache) {
	return &Cache{
		rosize: rosz,
		rodata: make(map[int64]*item, rosz),
		rwsize: rwsz,
		rwdata: make(map[int64]*item, rwsz),
		dbpath: dir,
		mapmtx: &sync.RWMutex{},
		rsize:  rsz,
	}
}

// LoadRO fetches an epoch for reading. It will check for
// epochs loaded in write-mode because they are faster.
func (c *Cache) LoadRO(key int64) (epoch *Epoch, err error) {
	c.mapmtx.Lock()
	defer c.mapmtx.Unlock()

	if item, ok := c.rwdata[key]; ok {
		nextID := atomic.AddInt64(&c.nextID, 1)
		atomic.StoreInt64(&item.weight, nextID)
		return item.epoch, nil
	}

	if item, ok := c.rodata[key]; ok {
		nextID := atomic.AddInt64(&c.nextID, 1)
		atomic.StoreInt64(&item.weight, nextID)
		return item.epoch, nil
	}

	keystr := strconv.Itoa(int(key))
	dir := path.Join(c.dbpath, keystr)

	epoch, err = NewRO(dir, c.rsize)
	if err != nil {
		return nil, err
	}

	// add new item to the collection
	nextID := atomic.AddInt64(&c.nextID, 1)
	c.rodata[key] = &item{
		weight: nextID,
		epoch:  epoch,
	}

	c.enforceSizeRO()

	return epoch, nil
}

// LoadRW fetches an epoch for writing. It will make sure that
// the epoch is not already loaded in read-only mode.
func (c *Cache) LoadRW(key int64) (epoch *Epoch, err error) {
	c.mapmtx.Lock()
	defer c.mapmtx.Unlock()

	if item, ok := c.rodata[key]; ok {
		delete(c.rodata, key)
		item.epoch.Close()
	}

	if item, ok := c.rwdata[key]; ok {
		nextID := atomic.AddInt64(&c.nextID, 1)
		atomic.StoreInt64(&item.weight, nextID)
		return item.epoch, nil
	}

	keystr := strconv.Itoa(int(key))
	dir := path.Join(c.dbpath, keystr)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	epoch, err = NewRW(dir, c.rsize)
	if err != nil {
		return nil, err
	}

	// add new item to the collection
	nextID := atomic.AddInt64(&c.nextID, 1)
	c.rwdata[key] = &item{
		weight: nextID,
		epoch:  epoch,
	}

	c.enforceSizeRW()

	return epoch, nil
}

// Expire removes all epochs from cache which are older than given timestamp
// To remove all epochs, use ExpireAll (maximum int64 value) as the timestamp.
func (c *Cache) Expire(ts int64) {
	todo := make(map[int64]*item, c.rosize)

	c.mapmtx.Lock()
	for k, el := range c.rodata {
		if k < ts {
			todo[k] = el
			delete(c.rodata, k)
		}
	}
	c.mapmtx.Unlock()

	for _, el := range todo {
		el.epoch.Close()
	}
}

// Sync flushes all data to disk
func (c *Cache) Sync() (err error) {
	c.mapmtx.RLock()
	defer c.mapmtx.RUnlock()

	for _, el := range c.rwdata {
		if err := el.epoch.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// Close releases resources
func (c *Cache) Close() (err error) {
	c.mapmtx.Lock()
	defer c.mapmtx.Unlock()

	for _, el := range c.rwdata {
		if err := el.epoch.Close(); err != nil {
			return err
		}
	}

	for _, el := range c.rodata {
		if err := el.epoch.Close(); err != nil {
			return err
		}
	}

	return nil
}

// enforceSizeRO checks size limits for read-only epochs
func (c *Cache) enforceSizeRO() {
	c.enforceSize(c.rodata, c.rosize)
}

// enforceSizeRW checks size limits for read-write epochs
func (c *Cache) enforceSizeRW() {
	c.enforceSize(c.rwdata, c.rwsize)
}

// enforceSize checks size limits for given data map and size
func (c *Cache) enforceSize(data map[int64]*item, size int64) {
	for len(data) > int(size) {
		var minKey int64
		var minEl *item

		for k, el := range data {
			if minEl == nil || minEl.weight > el.weight {
				minEl = el
				minKey = k
			}
		}

		delete(data, minKey)
		minEl.epoch.Close()
	}
}
