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
	// AllItems removes all
	AllItems = math.MaxInt64
)

type item struct {
	value int64
	epoch *Epoch
}

// Cache is an LRU cache
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

// NewCache crates an LRU cache with given max size
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
		atomic.StoreInt64(&item.value, nextID)
		return item.epoch, nil
	}

	if item, ok := c.rodata[key]; ok {
		nextID := atomic.AddInt64(&c.nextID, 1)
		atomic.StoreInt64(&item.value, nextID)
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
		value: nextID,
		epoch: epoch,
	}

	c.enforceSize(true)

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
		atomic.StoreInt64(&item.value, nextID)
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
	c.rodata[key] = &item{
		value: nextID,
		epoch: epoch,
	}

	c.enforceSize(false)

	return epoch, nil
}

func (c *Cache) enforceSize(ronly bool) {
	var data map[int64]*item
	var size int64

	if ronly {
		data, size = c.rodata, c.rosize
	} else {
		data, size = c.rwdata, c.rwsize
	}

	for len(data) > int(size) {
		var minKey int64
		var minEl *item

		for k, el := range data {
			if minEl == nil || minEl.value > el.value {
				minEl = el
				minKey = k
			}
		}

		delete(data, minKey)
		minEl.epoch.Close()
	}
}

// Expire removes all epochs from cache
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

// Close closes the cache
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
