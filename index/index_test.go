package index

import (
	"os"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
)

const (
	dir = "/tmp/test-index"
)

func TestNewIndexRW(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	for j := 0; j < 3; j++ {
		i, err := NewRW(dir)
		if err != nil {
			t.Fatal(err)
		}

		if err := i.Close(); err != nil {
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
		i, err := NewRO(dir)
		if err != nil {
			t.Fatal(err)
		}

		if err := i.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureNode(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	i, err := NewRW(dir)
	if err != nil {
		t.Fatal(err)
	}

	sets := [][]string{
		{"a", "b", "c"},
		{"a", "b", "d"},
		{"a", "e", "c"},
		{"a", "e", "d"},
	}

	for j, f := range sets {
		n, err := i.Ensure(f)
		if err != nil {
			t.Fatal(err)
		}

		if int64(j) != n.RecordID ||
			!reflect.DeepEqual(f, n.Fields) {
			t.Fatal("invalid node")
		}
	}

	if err := i.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestFindOne(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	i, err := NewRW(dir)
	if err != nil {
		t.Fatal(err)
	}

	sets := [][]string{
		{"a", "b", "c"},
		{"a", "b", "d"},
		{"a", "e", "c"},
		{"a", "e", "d"},
	}

	for _, f := range sets {
		n, err := i.Ensure(f)
		if err != nil {
			t.Fatal(err)
		}

		m, err := i.FindOne(f)
		if err != nil {
			t.Fatal(err)
		}

		if n != m {
			t.Fatal("cannot find")
		}
	}

	if err := i.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestFindFast(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	i, err := NewRW(dir)
	if err != nil {
		t.Fatal(err)
	}

	sets := [][]string{
		{"a", "b", "c"},
		{"a", "b", "d"},
		{"a", "e", "c"},
		{"a", "e", "d"},
	}

	for _, f := range sets {
		if _, err := i.Ensure(f); err != nil {
			t.Fatal(err)
		}
	}

	ns, err := i.Find([]string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}

	if len(ns) != 1 || ns[0].RecordID != 0 {
		t.Fatal("wrong result")
	}

	ns, err = i.Find([]string{"a", "b", "z"})
	if err != nil {
		t.Fatal(err)
	}

	if len(ns) != 0 {
		t.Fatal("wrong result")
	}

	if err := i.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestFindSlow(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	i, err := NewRW(dir)
	if err != nil {
		t.Fatal(err)
	}

	sets := [][]string{
		{"a", "b", "c"},
		{"a", "b", "d"},
		{"a", "e", "c"},
		{"a", "e", "d"},
	}

	for _, f := range sets {
		if _, err := i.Ensure(f); err != nil {
			t.Fatal(err)
		}
	}

	// wildcard for last field
	ns, err := i.Find([]string{"a", "b", "*"})
	if err != nil {
		t.Fatal(err)
	}

	if len(ns) != 2 ||
		(ns[0].RecordID != 0 && ns[1].RecordID != 0) ||
		(ns[0].RecordID != 1 && ns[1].RecordID != 1) {
		t.Fatal("wrong result")
	}

	// wildcard for a mid field
	ns, err = i.Find([]string{"a", "*", "c"})
	if err != nil {
		t.Fatal(err)
	}

	if len(ns) != 2 ||
		(ns[0].RecordID != 0 && ns[1].RecordID != 0) ||
		(ns[0].RecordID != 2 && ns[1].RecordID != 2) {
		t.Fatal("wrong result")
	}

	// unknown value for last field
	ns, err = i.Find([]string{"a", "*", "z"})
	if err != nil {
		t.Fatal(err)
	}

	if len(ns) != 0 {
		t.Fatal("wrong result")
	}

	// unknown value for a mid field
	ns, err = i.Find([]string{"z", "*", "c"})
	if err != nil {
		t.Fatal(err)
	}

	if len(ns) != 0 {
		t.Fatal("wrong result")
	}

	if err := i.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkAdd(b *testing.B) {
	if err := os.RemoveAll(dir); err != nil {
		b.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		b.Fatal(err)
	}

	i, err := NewRW(dir)
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
			if _, err := i.Ensure(f); err != nil {
				b.Fatal(err)
			}
		}
	})

	if err := i.Close(); err != nil {
		b.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkFindOne(b *testing.B) {
	if err := os.RemoveAll(dir); err != nil {
		b.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		b.Fatal(err)
	}

	i, err := NewRW(dir)
	if err != nil {
		b.Fatal(err)
	}

	sets := make([][]string, b.N)
	for j := 0; j < b.N; j++ {
		f := []string{"a", "b", "c"}
		f[j%3] = f[j%3] + strconv.Itoa(j&1000)
		if _, err := i.Ensure(f); err != nil {
			b.Fatal(err)
		}

		sets[j] = f
	}

	var j int64

	b.SetParallelism(1000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c := atomic.AddInt64(&j, 1) - 1
			i.FindOne(sets[c])
		}
	})

	if err := i.Close(); err != nil {
		b.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkFindFast(b *testing.B) {
	if err := os.RemoveAll(dir); err != nil {
		b.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		b.Fatal(err)
	}

	i, err := NewRW(dir)
	if err != nil {
		b.Fatal(err)
	}

	sets := make([][]string, b.N)
	for j := 0; j < b.N; j++ {
		f := []string{"a", "b", "c"}
		f[j%3] = f[j%3] + strconv.Itoa(j&100)
		if _, err := i.Ensure(f); err != nil {
			b.Fatal(err)
		}

		sets[j] = f
	}

	var j int64

	b.SetParallelism(1000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c := atomic.AddInt64(&j, 1) - 1
			i.Find(sets[c])
		}
	})

	if err := i.Close(); err != nil {
		b.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkFindSlow(b *testing.B) {
	if err := os.RemoveAll(dir); err != nil {
		b.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		b.Fatal(err)
	}

	i, err := NewRW(dir)
	if err != nil {
		b.Fatal(err)
	}

	sets := make([][]string, b.N)
	queries := make([][]string, b.N)
	for j := 0; j < b.N; j++ {
		f := []string{"a", "b", "c"}
		f[j%3] = f[j%3] + strconv.Itoa(j&1000)
		if _, err := i.Ensure(f); err != nil {
			b.Fatal(err)
		}
		sets[j] = f

		q := []string{"a", "b", "*"}
		q[j%2] = q[j%2] + strconv.Itoa(j&1000)
		queries[j] = q
	}

	var j int64

	b.SetParallelism(1000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c := atomic.AddInt64(&j, 1) - 1
			i.Find(queries[c])
		}
	})

	if err := i.Close(); err != nil {
		b.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		b.Fatal(err)
	}
}
