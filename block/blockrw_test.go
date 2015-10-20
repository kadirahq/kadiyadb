package block

import (
	"os"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/kadirahq/kadiyadb-protocol"
)

var (
	tmpdirrw = "/tmp/test-rwblock/"
)

func setuprw(t testing.TB) func() {
	if err := os.RemoveAll(tmpdirrw); err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(tmpdirrw, 0777); err != nil {
		t.Fatal(err)
	}

	return func() {
		if err := os.RemoveAll(tmpdirrw); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNewRW(t *testing.T) {
	defer setuprw(t)()

	for i := 0; i < 3; i++ {
		b, err := NewRW(tmpdirrw, 5)
		if err != nil {
			t.Fatal(err)
		}

		if len(b.records) != 0 {
			t.Fatal("wrong length")
		}

		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOpenRW(t *testing.T) {
	defer setuprw(t)()

	b, err := NewRW(tmpdirrw, 5)
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

	b, err = NewRW(tmpdirrw, 5)
	if err != nil {
		t.Fatal(err)
	}

	if len(b.records) < 6 {
		t.Fatal("wrong length")
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncRW(t *testing.T) {
	defer setuprw(t)()

	b, err := NewRW(tmpdirrw, 5)
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Sync(); err != nil {
		t.Fatal(err)
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTrackerRW(t *testing.T) {
	defer setuprw(t)()

	b, err := NewRW(tmpdirrw, 5)
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

	g := b.records[:2]
	if !reflect.DeepEqual(e, g) {
		t.Fatal("wrong values")
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFetcherRW(t *testing.T) {
	defer setuprw(t)()

	b, err := NewRW(tmpdirrw, 5)
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

	r0, _ := b.Fetch(0, 0, 5)
	r1, _ := b.Fetch(1, 0, 5)
	g := [][]protocol.Point{r0, r1}
	if !reflect.DeepEqual(e, g) {
		t.Fatal("wrong values")
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestImplRW(t *testing.T) {
	// throws error if it doesn't
	var _ Block = &RWBlock{}
}

func BenchTrackRW(b *testing.B, rs int64) {
	defer setuprw(b)()

	b1, err := NewRW(tmpdirrw, 100)
	if err != nil {
		b.Fatal(err)
	}

	defer b1.Close()

	N := int64(b.N)
	b.ResetTimer()

	for {
		n := atomic.AddInt64(&N, -1)
		if n < 0 {
			return
		}

		rid := n / 100
		pid := n % 100

		if err := b1.Track(rid, pid, 1, 1); err != nil {
			b.Fatal(err)
		}
	}
}

// time/op should not change!
func BenchmarkTrackRW1k(b *testing.B) { BenchTrackRW(b, 1000) }
func BenchmarkTrackRW1M(b *testing.B) { BenchTrackRW(b, 1000000) }

func BenchTrackRWP(b *testing.B, rs int64) {
	defer setuprw(b)()

	b1, err := NewRW(tmpdirrw, 100)
	if err != nil {
		b.Fatal(err)
	}

	defer b1.Close()

	N := int64(b.N)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&N, -1)
			if n < 0 {
				return
			}

			rid := n / 100
			pid := n % 100

			if err := b1.Track(rid, pid, 1, 1); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// time/op should not change!
func BenchmarkTrackRW1kP(b *testing.B) { BenchTrackRWP(b, 1000) }
func BenchmarkTrackRW1MP(b *testing.B) { BenchTrackRWP(b, 1000000) }

func BenchFetchRW(b *testing.B, ps int64) {
	defer setuprw(b)()

	b1, err := NewRW(tmpdirrw, ps)
	if err != nil {
		b.Fatal(err)
	}

	defer b1.Close()

	// create a record
	if err := b1.Track(0, 0, 0, 0); err != nil {
		b.Fatal(err)
	}

	N := int64(b.N)
	b.ResetTimer()

	for {
		n := atomic.AddInt64(&N, -1)
		if n < 0 {
			return
		}

		if _, err := b1.Fetch(0, 0, ps); err != nil {
			b.Fatal(err)
		}
	}
}

// time/op should not change!
func BenchmarkFetchRW1k(b *testing.B) { BenchFetchRW(b, 1000) }
func BenchmarkFetchRW1M(b *testing.B) { BenchFetchRW(b, 1000000) }

func BenchFetchRWP(b *testing.B, ps int64) {
	defer setuprw(b)()

	b1, err := NewRW(tmpdirrw, ps)
	if err != nil {
		b.Fatal(err)
	}

	defer b1.Close()

	// create a record
	if err := b1.Track(0, 0, 0, 0); err != nil {
		b.Fatal(err)
	}

	N := int64(b.N)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&N, -1)
			if n < 0 {
				return
			}

			if _, err := b1.Fetch(0, 0, ps); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// time/op should not change!
func BenchmarkFetchRW1kP(b *testing.B) { BenchFetchRWP(b, 1000) }
func BenchmarkFetchRW1MP(b *testing.B) { BenchFetchRWP(b, 1000000) }
