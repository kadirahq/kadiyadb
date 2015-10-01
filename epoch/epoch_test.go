package epoch

import (
	"os"
	"strconv"
	"sync/atomic"
	"testing"
)

const (
	dir = "/tmp/test-epoch"
)

func TestNewIndexRW(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	for j := 0; j < 3; j++ {
		e, err := NewRW(dir, 10)
		if err != nil {
			t.Fatal(err)
		}

		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestNewIndexRO(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	for j := 0; j < 3; j++ {
		e, err := NewRO(dir, 10)
		if err != nil {
			t.Fatal(err)
		}

		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestTrackValue(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	e, err := NewRW(dir, 5)
	if err != nil {
		t.Fatal(err)
	}

	sets := [][]string{
		[]string{"a", "b", "c"},
		[]string{"a", "b", "d"},
		[]string{"a", "c", "e"},
	}

	for i, fields := range sets {
		for j := 0; j < 5; j++ {
			if err := e.Track(int64(j), fields, float64(i+1), uint64(i+1)); err != nil {
				t.Fatal(err)
			}
		}
	}

	// TODO compare all index nodes
	// TODO compare all block points

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestFetchFast(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	e, err := NewRW(dir, 5)
	if err != nil {
		t.Fatal(err)
	}

	sets := [][]string{
		[]string{"a", "b", "c"},
		[]string{"a", "b", "d"},
		[]string{"a", "c", "e"},
	}

	for i, fields := range sets {
		for j := 0; j < 5; j++ {
			if err := e.Track(int64(j), fields, float64(i+1), uint64(i+1)); err != nil {
				t.Fatal(err)
			}
		}
	}

	// TODO fetch without wildcards

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestFetchSlow(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	e, err := NewRW(dir, 5)
	if err != nil {
		t.Fatal(err)
	}

	sets := [][]string{
		[]string{"a", "b", "c"},
		[]string{"a", "b", "d"},
		[]string{"a", "c", "e"},
	}

	for i, fields := range sets {
		for j := 0; j < 5; j++ {
			if err := e.Track(int64(j), fields, float64(i+1), uint64(i+1)); err != nil {
				t.Fatal(err)
			}
		}
	}

	// TODO fetch with wildcards

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkTrackValue(b *testing.B) {
	if err := os.RemoveAll(dir); err != nil {
		b.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		b.Fatal(err)
	}

	e, err := NewRW(dir, 100)
	if err != nil {
		b.Fatal(err)
	}

	sets := make([][]string, b.N)
	for j := 0; j < b.N; j++ {
		sets[j] = []string{"a", "b", "c"}
		sets[j][j%3] = sets[j][j%3] + strconv.Itoa(j)
	}

	var j int64

	b.SetParallelism(1000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c := atomic.AddInt64(&j, 1) - 1
			f := sets[c]
			p := c % 100

			if err := e.Track(int64(p), f, 1, 1); err != nil {
				b.Fatal(err)
			}
		}
	})

	if err := e.Close(); err != nil {
		b.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		b.Fatal(err)
	}
}
