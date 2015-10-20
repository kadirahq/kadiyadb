package block

import (
	"os"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/kadirahq/kadiyadb-protocol"
)

var (
	tmpdirro = "/tmp/test-roblock/"
)

func setupro(t testing.TB) func() {
	if err := os.RemoveAll(tmpdirro); err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(tmpdirro, 0777); err != nil {
		t.Fatal(err)
	}

	return func() {
		if err := os.RemoveAll(tmpdirro); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNewRO(t *testing.T) {
	defer setupro(t)()

	for i := 0; i < 3; i++ {
		b, err := NewRO(tmpdirro, 5)
		if err != nil {
			t.Fatal(err)
		}

		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOpenRO(t *testing.T) {
	defer setupro(t)()

	b, err := NewRW(tmpdirro, 5)
	if err != nil {
		t.Fatal(err)
	}

	if len(b.records) != 0 {
		t.Fatal("wrong length")
	}

	if err := b.Track(5, 0, 1, 1); err != nil {
		t.Fatal(err)
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2, err := NewRO(tmpdirro, 5)
	if err != nil {
		t.Fatal(err)
	}

	if err := b2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFetcherRO(t *testing.T) {
	defer setupro(t)()

	b, err := NewRW(tmpdirro, 5)
	if err != nil {
		t.Fatal(err)
	}

	e := make([][]protocol.Point, 2)

	var rid, pid int64
	for rid = 0; rid < 2; rid++ {
		e[rid] = make([]protocol.Point, 5)

		for pid = 0; pid < 5; pid++ {
			total := float64(rid)
			count := float64(pid)
			e[rid][pid] = protocol.Point{total, count}

			if err := b.Track(rid, pid, total, count); err != nil {
				t.Fatal(err)
			}
		}
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2, err := NewRO(tmpdirro, 5)
	if err != nil {
		t.Fatal(err)
	}

	r0, _ := b2.Fetch(0, 0, 5)
	r1, _ := b2.Fetch(1, 0, 5)
	g := [][]protocol.Point{r0, r1}
	if !reflect.DeepEqual(e, g) {
		t.Fatal("wrong values")
	}

	if err := b2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestImplRO(t *testing.T) {
	// throws error if it doesn't
	var _ Block = &ROBlock{}
}

func BenchFetchRO(b *testing.B, ps int64) {
	defer setupro(b)()

	b1, err := NewRW(tmpdirro, ps)
	if err != nil {
		b.Fatal(err)
	}

	// create a record
	if err := b1.Track(0, 0, 0, 0); err != nil {
		b.Fatal(err)
	}

	if err := b1.Close(); err != nil {
		b.Fatal(err)
	}

	b2, err := NewRW(tmpdirro, ps)
	if err != nil {
		b.Fatal(err)
	}

	defer b2.Close()

	N := int64(b.N)
	b.ResetTimer()

	for {
		n := atomic.AddInt64(&N, -1)
		if n < 0 {
			return
		}

		if _, err := b2.Fetch(0, 0, ps); err != nil {
			b.Fatal(err)
		}
	}
}

// time/op should not change!
func BenchmarkFetchRO1k(b *testing.B) { BenchFetchRO(b, 1000) }
func BenchmarkFetchRO1M(b *testing.B) { BenchFetchRO(b, 1000000) }

func BenchFetchROP(b *testing.B, ps int64) {
	defer setupro(b)()

	b1, err := NewRW(tmpdirro, ps)
	if err != nil {
		b.Fatal(err)
	}

	// create a record
	if err := b1.Track(0, 0, 0, 0); err != nil {
		b.Fatal(err)
	}

	if err := b1.Close(); err != nil {
		b.Fatal(err)
	}

	b2, err := NewRW(tmpdirro, ps)
	if err != nil {
		b.Fatal(err)
	}

	defer b2.Close()

	N := int64(b.N)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&N, -1)
			if n < 0 {
				return
			}

			if _, err := b2.Fetch(0, 0, ps); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// time/op should not change!
func BenchmarkFetchRO1kP(b *testing.B) { BenchFetchROP(b, 1000) }
func BenchmarkFetchRO1MP(b *testing.B) { BenchFetchROP(b, 1000000) }
