package epoch

import (
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/kadirahq/kadiyadb/block"
	"github.com/kadirahq/kadiyadb/index"
)

const (
	dir = "/tmp/test-epoch"
)

type Nodes []*index.Node

func (a Nodes) Len() int           { return len(a) }
func (a Nodes) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Nodes) Less(i, j int) bool { return a[i].RecordID < a[j].RecordID }

type Series [][]block.Point

func (a Series) Len() int           { return len(a) }
func (a Series) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Series) Less(i, j int) bool { return a[i][0].Total < a[j][0].Total }

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

	type test struct {
		query  []string
		nodes  Nodes
		points Series
	}

	tests := []test{
		test{
			query: []string{"a"},
			nodes: Nodes{
				{RecordID: 0, Fields: []string{"a"}},
			},
			points: Series{
				{{6, 6}, {6, 6}, {6, 6}, {6, 6}, {6, 6}},
			},
		},
		test{
			query: []string{"a", "b"},
			nodes: Nodes{
				{RecordID: 1, Fields: []string{"a", "b"}},
			},
			points: Series{
				{{3, 3}, {3, 3}, {3, 3}, {3, 3}, {3, 3}},
			},
		},
		test{
			query: []string{"a", "b", "c"},
			nodes: Nodes{
				{RecordID: 2, Fields: []string{"a", "b", "c"}},
			},
			points: Series{
				{{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}},
			},
		},
		test{
			query: []string{"a", "b", "d"},
			nodes: Nodes{
				{RecordID: 3, Fields: []string{"a", "b", "d"}},
			},
			points: Series{
				{{2, 2}, {2, 2}, {2, 2}, {2, 2}, {2, 2}},
			},
		},
		test{
			query: []string{"a", "c"},
			nodes: Nodes{
				{RecordID: 4, Fields: []string{"a", "c"}},
			},
			points: Series{
				{{3, 3}, {3, 3}, {3, 3}, {3, 3}, {3, 3}},
			},
		},
		test{
			query: []string{"a", "c", "e"},
			nodes: Nodes{
				{RecordID: 5, Fields: []string{"a", "c", "e"}},
			},
			points: Series{
				{{3, 3}, {3, 3}, {3, 3}, {3, 3}, {3, 3}},
			},
		},
	}

	for _, tst := range tests {
		points, nodes, err := e.Fetch(0, 5, tst.query)
		if err != nil {
			t.Fatal(err)
		}

		sort.Sort(Nodes(nodes))
		if !reflect.DeepEqual(Nodes(nodes), tst.nodes) {
			t.Fatal("wrong nodes")
		}

		sort.Sort(Series(points))
		if !reflect.DeepEqual(Series(points), tst.points) {
			t.Fatal("wrong points")
		}
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestFetchFast(t *testing.T) {
	// NOTE checked here
	TestTrackValue(t)
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
		[]string{"a", "e", "c"},
	}

	for i, fields := range sets {
		for j := 0; j < 5; j++ {
			if err := e.Track(int64(j), fields, float64(i+1), uint64(i+1)); err != nil {
				t.Fatal(err)
			}
		}
	}

	type test struct {
		query  []string
		nodes  Nodes
		points Series
	}

	tests := []test{
		test{
			query: []string{"a", "b", "*"},
			nodes: Nodes{
				{RecordID: 2, Fields: []string{"a", "b", "c"}},
				{RecordID: 3, Fields: []string{"a", "b", "d"}},
			},
			points: Series{
				{{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}},
				{{2, 2}, {2, 2}, {2, 2}, {2, 2}, {2, 2}},
			},
		},
		test{
			query: []string{"a", "*", "c"},
			nodes: Nodes{
				{RecordID: 2, Fields: []string{"a", "b", "c"}},
				{RecordID: 5, Fields: []string{"a", "e", "c"}},
			},
			points: Series{
				{{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}},
				{{3, 3}, {3, 3}, {3, 3}, {3, 3}, {3, 3}},
			},
		},
		test{
			query: []string{"a", "*", "*"},
			nodes: Nodes{
				{RecordID: 2, Fields: []string{"a", "b", "c"}},
				{RecordID: 3, Fields: []string{"a", "b", "d"}},
				{RecordID: 5, Fields: []string{"a", "e", "c"}},
			},
			points: Series{
				{{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}},
				{{2, 2}, {2, 2}, {2, 2}, {2, 2}, {2, 2}},
				{{3, 3}, {3, 3}, {3, 3}, {3, 3}, {3, 3}},
			},
		},
	}

	for _, tst := range tests {
		points, nodes, err := e.Fetch(0, 5, tst.query)
		if err != nil {
			t.Fatal(err)
		}

		sort.Sort(Nodes(nodes))
		if !reflect.DeepEqual(Nodes(nodes), tst.nodes) {
			t.Fatal("wrong nodes")
		}

		sort.Sort(Series(points))
		if !reflect.DeepEqual(Series(points), tst.points) {
			t.Fatal("wrong points")
		}
	}

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
