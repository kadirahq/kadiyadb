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

func TestAddNode(t *testing.T) {
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
		n, err := i.Add(f)
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
		n, err := i.Add(f)
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
		if _, err := i.Add(f); err != nil {
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
		if _, err := i.Add(f); err != nil {
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
		sets[j] = []string{"a", "b", "c", "d"}
		sets[j][j%4] = sets[j][j%4] + strconv.Itoa(j)
	}

	var j int64

	b.SetParallelism(1000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c := atomic.AddInt64(&j, 1) - 1
			f := sets[c]
			if _, err := i.Add(f); err != nil {
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
		f := []string{"a", "b", "c", "d"}
		f[j%4] = f[j%4] + strconv.Itoa(j)
		if _, err := i.Add(f); err != nil {
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
			f := sets[c]
			i.FindOne(f)
		}
	})

	if err := i.Close(); err != nil {
		b.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		b.Fatal(err)
	}
}
