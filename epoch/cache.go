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
	size  int
	data  map[int64]*item
	base  string
	rsz   int64
	next  int64
	ronly bool
	mutx  *sync.RWMutex
}

// NewCache crates an LRU cache with given max size
func NewCache(sz int, dir string, rsz int64, ro bool) (c *Cache) {
	return &Cache{
		size:  sz,
		data:  make(map[int64]*item, sz),
		base:  dir,
		rsz:   rsz,
		ronly: ro,
		mutx:  &sync.RWMutex{},
	}
}

// Fetch fetches an epoch from the cache if it's available.
// If the epoch is not available, it will be loaded from the disk.
// It will automatically remove least used epoch if it's full
func (c *Cache) Fetch(k int64) (epoch *Epoch, err error) {
	// fast path!!
	c.mutx.RLock()
	if el, ok := c.data[k]; ok {
		el.value = c.nextID()
		c.mutx.RUnlock()
		return el.epoch, nil
	}
	c.mutx.RUnlock()

	// slow path!!
	// load epoch
	c.mutx.Lock()
	defer c.mutx.Unlock()

	// TODO add mutex on item struct to avoid total lockdown
	//      loading an epoch takes time, it shouldn't block
	//      other operations. Check the index for an example.

	if el, ok := c.data[k]; ok {
		el.value = c.nextID()
		return el.epoch, nil
	}

	key := strconv.Itoa(int(k))
	dir := path.Join(c.base, key)

	if c.ronly {
		epoch, err = NewRO(dir, c.rsz)
	} else {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}

		epoch, err = NewRW(dir, c.rsz)
	}

	if err != nil {
		return nil, err
	}

	// add new item to the collection
	c.data[k] = &item{
		value: c.nextID(),
		epoch: epoch,
	}

	var minKey int64
	var minEl *item
	// remove least used item
	if len(c.data) > c.size {
		for k, el := range c.data {
			if minEl == nil || minEl.value > el.value {
				minEl = el
				minKey = k
			}
		}

		delete(c.data, minKey)
		minEl.epoch.Close()
	}

	return epoch, nil
}

// Expire removes all epochs from cache
func (c *Cache) Expire(ts int64) {
	todo := make(map[int64]*item, c.size)

	c.mutx.Lock()
	for k, el := range c.data {
		if k < ts {
			todo[k] = el
			delete(c.data, k)
		}
	}
	c.mutx.Unlock()

	for _, el := range todo {
		el.epoch.Close()
	}
}

// Close closes the cache
func (c *Cache) Close() (err error) {
	c.mutx.Lock()
	defer c.mutx.Unlock()

	for _, el := range c.data {
		if err := el.epoch.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (c *Cache) nextID() (id int64) {
	return atomic.AddInt64(&c.next, 1)
}
