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
	ErrEmpty = errors.New("cannot create empty cache")

	// ErrExists is returned when user adds an item but the key already exists
	ErrExists = errors.New("item already exists in the cache")
)

// Cache is an cache of arbitrary items. The cache can hold only a fixed
// number of items. When more items are added, earliest will be removed and
// sent to an overflow channel.
// TODO: implement an LRU cache
type Cache interface {
	// Add adds an item to the cache
	Add(key int64, val interface{}) (err error)

	// Get returns an item from the cache by it's key
	Get(key int64) (val interface{}, err error)

	// Out returns the overflow channel
	Out() (ch <-chan interface{})

	// Flush removes all items from cache and returns them
	Flush() (data map[int64]interface{})

	// Length returns currently available number of items
	Length() (length int)
}

type cache struct {
	opts *Options              // options
	data map[int64]interface{} // data items
	keys []int64               // data keys in order
	head int                   // head position (removed here)
	tail int                   // tail position (added here)
	lnth int                   // current length
	mutx *sync.Mutex           // write mutex
	outc chan interface{}      // overflow channel
}

// Options has parameters required for creating a `Cache`
type Options struct {
	Size int // number of items to store in the cache
}

// New function creates an lru cache with given size.
func New(options *Options) (_c Cache, err error) {
	if options.Size <= 0 {
		logger.Log(LoggerPrefix, ErrEmpty)
		return nil, ErrEmpty
	}

	c := &cache{
		opts: options,
		data: make(map[int64]interface{}, options.Size),
		keys: make([]int64, options.Size),
		mutx: &sync.Mutex{},
		outc: make(chan interface{}),
	}

	return c, nil
}

func (c *cache) Add(key int64, val interface{}) (err error) {
	c.mutx.Lock()
	defer c.mutx.Unlock()

	if _, ok := c.data[key]; ok {
		return ErrExists
	}

	if c.lnth == c.opts.Size {
		k := c.keys[c.head]
		v := c.data[k]
		c.outc <- v

		delete(c.data, k)
		c.head = (c.head + 1) % c.opts.Size
		c.lnth--
	}

	c.keys[c.tail] = key
	c.data[key] = val
	c.tail = (c.tail + 1) % c.opts.Size
	c.lnth++

	return nil
}

func (c *cache) Get(key int64) (val interface{}, err error) {
	val, ok := c.data[key]
	if !ok {
		logger.Log(LoggerPrefix, ErrMissing)
		return nil, ErrMissing
	}

	return val, nil
}

func (c *cache) Out() (ch <-chan interface{}) {
	return c.outc
}

func (c *cache) Flush() (data map[int64]interface{}) {
	c.mutx.Lock()
	defer c.mutx.Unlock()

	data = c.data
	c.data = make(map[int64]interface{}, c.opts.Size)
	c.keys = make([]int64, c.opts.Size)
	c.head = 0
	c.tail = 0
	c.lnth = 0

	return data
}

func (c *cache) Length() (length int) {
	return c.lnth
}
