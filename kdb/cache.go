package kdb

// Cache TODO
type Cache interface {
	Add(k int64, e Epoch)
	Get(k int64) (e Epoch, ok bool)
	Peek(k int64) (e Epoch, ok bool)

	Resize(sz int)
	Purge()
}

type element struct {
	id    int64
	epoch Epoch
}

type cache struct {
	size  int
	data  map[int64]*element
	evict func(k int64, e Epoch)
	next  int64
}

type evictFn func(k int64, e Epoch)

// NewCache crates a leaky cache with given max size
func NewCache(size int, fn evictFn) (c Cache) {
	data := make(map[int64]*element, size)

	return &cache{
		size:  size,
		data:  data,
		evict: fn,
	}
}

func (c *cache) Add(k int64, e Epoch) {
	c.data[k] = &element{epoch: e, id: c.next}
	c.next++

	if len(c.data) > c.size {
		c.pop()
	}
}

func (c *cache) Get(k int64) (e Epoch, ok bool) {
	el, ok := c.data[k]
	if ok {
		el.id = c.next
		c.next++
		return el.epoch, true
	}

	return nil, false
}

func (c *cache) Peek(k int64) (e Epoch, ok bool) {
	el, ok := c.data[k]
	if ok {
		return el.epoch, true
	}

	return nil, false
}

func (c *cache) Resize(sz int) {
	c.size = sz

	for len(c.data) > c.size {
		c.pop()
	}
}

func (c *cache) Purge() {
	data := c.data

	c.data = make(map[int64]*element, c.size)
	for k, el := range data {
		c.evict(k, el.epoch)
	}
}

func (c *cache) pop() {
	var minKey int64
	var minEl *element

	for k, el := range c.data {
		if minEl == nil || minEl.id > el.id {
			minEl = el
			minKey = k
		}
	}

	delete(c.data, minKey)
	c.evict(minKey, minEl.epoch)
}
