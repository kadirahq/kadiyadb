package cache

import (
	"errors"
	"sync"

	"github.com/meteorhacks/kadiradb-core/utils/logger"
)

const (
	// LoggerPrefix will be used to prefix debug logs
	LoggerPrefix = "CACHE"
)

var (
	// ErrMissing is returned when requested key is not available in cache
	ErrMissing = errors.New("key does not exist")

	// ErrEmpty is returned when user tries to create a Cache
	// with size set to zero (or not set in options which means zero)
	ErrEmpty = errors.New("cannot create empty lru cache")
)

// Cache is an LRU cache of arbitrary items. The cache can hold only a fixed
// number of items. When more items are added, they'll be removed and sent
// to an overflow channel.
type Cache interface {
	Add(key int64, val interface{}) (err error)
	Get(key int64) (val interface{}, err error)
	Del(key int64) (val interface{}, err error)
	Out() (ch <-chan interface{})
	Flush() (data map[int64]interface{})
	Length() (length int)
}

type cache struct {
	data map[int64]interface{}
	keys []int64
	head int
	tail int
	lnth int
	size int
	mutx *sync.Mutex
	outc chan interface{}
}

// Options has parameters required for creating a `Cache`
type Options struct {
	Size int
}

// New function creates an lru cache with given size.
func New(options *Options) (_c Cache, err error) {
	if options.Size <= 0 {
		logger.Log(LoggerPrefix, ErrEmpty)
		return nil, ErrEmpty
	}

	c := &cache{
		data: make(map[int64]interface{}, options.Size),
		keys: make([]int64, options.Size),
		size: options.Size,
		mutx: &sync.Mutex{},
		outc: make(chan interface{}),
	}

	return c, nil
}

func (c *cache) Add(key int64, val interface{}) (err error) {
	c.mutx.Lock()
	defer c.mutx.Unlock()

	if _, ok := c.data[key]; ok {
		c.del(key)
	}

	if c.lnth == c.size {
		k := c.keys[c.head]
		v := c.data[k]
		c.outc <- v
		c.del(k)
	}

	c.keys[c.tail] = key
	c.data[key] = val
	c.tail = (c.tail + 1) % c.size
	c.lnth++

	return nil
}

func (c *cache) Get(key int64) (val interface{}, err error) {
	val, ok := c.data[key]
	if !ok {
		logger.Log(LoggerPrefix, ErrMissing)
		return nil, ErrMissing
	}

	c.del(key)
	err = c.Add(key, val)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	return val, nil
}

func (c *cache) Del(key int64) (val interface{}, err error) {
	val, ok := c.data[key]
	if !ok {
		logger.Log(LoggerPrefix, ErrMissing)
		return nil, ErrMissing
	}

	c.del(key)

	return val, nil
}

func (c *cache) Out() (ch <-chan interface{}) {
	return c.outc
}

func (c *cache) Flush() (data map[int64]interface{}) {
	c.mutx.Lock()
	defer c.mutx.Unlock()

	data = c.data
	c.data = make(map[int64]interface{}, c.size)

	return data
}

func (c *cache) Length() (length int) {
	return c.lnth
}

func (c *cache) del(key int64) {
	delete(c.data, key)
	c.head = (c.head + 1) % c.size
	c.lnth--
}
