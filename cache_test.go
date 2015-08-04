package kadiyadb

import (
	"fmt"
	"testing"
)

func TestCache(t *testing.T) {
	var evictKey int64
	var evictVal Epoch
	evictFn := func(k int64, e Epoch) {
		evictKey = k
		evictVal = e
	}

	c := NewCache(2, evictFn)
	cc := c.(*cache)
	ok := true

	c.Add(10, &epoch{options: &EpochOptions{Path: "p1"}})
	c.Add(20, &epoch{options: &EpochOptions{Path: "p2"}})

	if len(cc.data) != cc.size ||
		cc.data[10].id != 0 ||
		cc.data[20].id != 1 {
		t.Fatal("incorrect ids")
	}

	_, ok = c.Peek(10)
	if !ok {
		t.Fatal("element missing")
	}

	if len(cc.data) != cc.size ||
		cc.data[10].id != 0 ||
		cc.data[20].id != 1 {
		t.Fatal("incorrect ids")
	}

	_, ok = c.Get(10)
	if !ok {
		t.Fatal("element missing")
	}

	if len(cc.data) != cc.size ||
		cc.data[10].id != 2 ||
		cc.data[20].id != 1 {
		t.Fatal("incorrect ids")
	}

	c.Add(30, &epoch{options: &EpochOptions{Path: "p3"}})
	if evictKey != 20 || evictVal.(*epoch).options.Path != "p2" {
		fmt.Println(evictKey)
		t.Fatal("incorrect evict")
	}

	if len(cc.data) != cc.size ||
		cc.data[10].id != 2 ||
		cc.data[30].id != 3 {
		t.Fatal("incorrect ids")
	}
}

func BenchCacheAddSize(b *testing.B, sz int) {
	c := NewCache(sz, func(k int64, e Epoch) {})
	e := &epoch{}

	var i int64
	var N = int64(b.N)

	b.ResetTimer()
	for i = 0; i < N; i++ {
		c.Add(i, e)
	}
}

func BenchmarkCacheAddSize5(b *testing.B)  { BenchCacheAddSize(b, 5) }
func BenchmarkCacheAddSize10(b *testing.B) { BenchCacheAddSize(b, 10) }
func BenchmarkCacheAddSize50(b *testing.B) { BenchCacheAddSize(b, 50) }

func BenchCacheGetSize(b *testing.B, sz int) {
	c := NewCache(sz, func(k int64, e Epoch) {})
	e := &epoch{}

	var i int64
	var N = int64(b.N)
	var S = int64(sz)

	for i = 0; i < S; i++ {
		c.Add(i, e)
	}

	b.ResetTimer()
	for i = 0; i < N; i++ {
		c.Get(i % S)
	}
}

func BenchmarkCacheGetSize5(b *testing.B)  { BenchCacheGetSize(b, 5) }
func BenchmarkCacheGetSize10(b *testing.B) { BenchCacheGetSize(b, 10) }
func BenchmarkCacheGetSize50(b *testing.B) { BenchCacheGetSize(b, 50) }

func BenchCachePeekSize(b *testing.B, sz int) {
	c := NewCache(sz, func(k int64, e Epoch) {})
	e := &epoch{}

	var i int64
	var N = int64(b.N)
	var S = int64(sz)

	for i = 0; i < S; i++ {
		c.Add(i, e)
	}

	b.ResetTimer()
	for i = 0; i < N; i++ {
		c.Peek(i % S)
	}
}

func BenchmarkCachePeekSize5(b *testing.B)  { BenchCachePeekSize(b, 5) }
func BenchmarkCachePeekSize10(b *testing.B) { BenchCachePeekSize(b, 10) }
func BenchmarkCachePeekSize50(b *testing.B) { BenchCachePeekSize(b, 50) }
