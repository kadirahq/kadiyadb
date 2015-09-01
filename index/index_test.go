package index

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/kadirahq/go-tools/logger"
)

const (
	dir = "/tmp/sfile"
)

var (
	OptionsSet = map[string]*Options{
		"rw-mode": &Options{Path: dir},
		"ro-mode": &Options{Path: dir, ROnly: true},
	}
)

func init() {
	logger.Disable("time")
}

func TestIndexFiles(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	i, err := New(OptionsSet["rw-mode"])
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c"}
	if err := i.Put(fields, 5); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := i.Close(); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	// a snapshot should be created when closing the index file
	// make sure that files exist and have correct data in them
	if _, err := ioutil.ReadFile(dir + "/index_snap_root_0"); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
	if _, err := ioutil.ReadFile(dir + "/index_snap_data_0"); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	// we shouldn't need these
	os.Remove(dir + "/index_0")
	os.Remove(dir + "/index_mdata")

	i, err = New(OptionsSet["ro-mode"])
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := i.Close(); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}

func TNewWithOptions(t *testing.T, o *Options) {
	err := os.RemoveAll(dir)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	i0, err := New(OptionsSet["rw-mode"])
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	err = i0.Close()
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	i0, err = New(o)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	err = i0.Close()
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	err = os.RemoveAll(dir)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}

func TestNew(t *testing.T) {
	for k, o := range OptionsSet {
		fmt.Println(" - testing with options:", k)
		TNewWithOptions(t, o)
	}
}

func TPutOneWithOptions(t *testing.T, o *Options) {
	err := os.RemoveAll(dir)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	// always use rw-mode for writing data
	i0, err := New(OptionsSet["rw-mode"])
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c"}
	err = i0.Put(fields, 100)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	item, err := i0.One(fields)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	err = i0.Close()
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	// finally, open with user provided options
	i0, err = New(o)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	item, err = i0.One(fields)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if item.Value != 100 {
		t.Fatal("wrong value")
	}

	err = i0.Close()
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	err = os.RemoveAll(dir)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}

func TestPutOne(t *testing.T) {
	for k, o := range OptionsSet {
		fmt.Println(" - testing with options:", k)
		TPutOneWithOptions(t, o)
	}
}

func TPutGetWithOptions(t *testing.T, o *Options) {
	err := os.RemoveAll(dir)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	// always use rw-mode for writing data
	i0, err := New(OptionsSet["rw-mode"])
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c"}
	err = i0.Put(fields, 100)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	err = i0.Close()
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	// finally, open with user provided options
	i0, err = New(o)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	items, err := i0.Get(fields)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if len(items) != 1 {
		t.Fatal("wrong count")
	}

	item := items[0]
	if item.Value != 100 {
		t.Fatal("wrong value")
	}

	err = i0.Close()
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	err = os.RemoveAll(dir)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}

func TestPutGet(t *testing.T) {
	for k, o := range OptionsSet {
		fmt.Println(" - testing with options:", k)
		TPutGetWithOptions(t, o)
	}
}

func BenchmarkPut(b *testing.B) {
	err := os.RemoveAll(dir)
	if err != nil {
		b.Fatal(err)
	}

	// always use rw-mode for writing data
	i0, err := New(OptionsSet["rw-mode"])
	if err != nil {
		b.Fatal(err)
	}

	fieldSets := make([][]string, b.N)
	for i := 0; i < b.N; i++ {
		fieldSets[i] = []string{"a", "b", "c", "d", "e"}

		p := i % 5
		fieldSets[i][p] = strconv.Itoa(i)
	}

	b.ResetTimer()
	b.SetParallelism(5000)

	var i int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&i, 1)
			fields := fieldSets[n-1]

			if err := i0.Put(fields, 100); err != nil {
				b.Fatal(err)
			}
		}
	})

	err = i0.Close()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkOne(b *testing.B) {
	err := os.RemoveAll(dir)
	if err != nil {
		b.Fatal(err)
	}

	// always use rw-mode for writing data
	i0, err := New(OptionsSet["rw-mode"])
	if err != nil {
		b.Fatal(err)
	}

	fieldSets := make([][]string, b.N)
	for i := 0; i < b.N; i++ {
		fieldSets[i] = []string{"a", "b", "c", "d", "e"}

		p := i % 5
		fieldSets[i][p] = strconv.Itoa(i)

		err = i0.Put(fieldSets[i], 100)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.SetParallelism(5000)

	var i int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&i, 1)
			fields := fieldSets[n-1]

			if _, err := i0.One(fields); err != nil {
				b.Fatal(err)
			}
		}
	})

	err = i0.Close()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkGet(b *testing.B) {
	err := os.RemoveAll(dir)
	if err != nil {
		b.Fatal(err)
	}

	// always use rw-mode for writing data
	i0, err := New(OptionsSet["rw-mode"])
	if err != nil {
		b.Fatal(err)
	}

	fieldSets := make([][]string, b.N)
	for i := 0; i < b.N; i++ {
		fieldSets[i] = []string{"a", "b", "c", "d", "e"}

		p := i % 5
		fieldSets[i][p] = strconv.Itoa(i)

		err = i0.Put(fieldSets[i], 100)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.SetParallelism(5000)

	var i int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&i, 1)
			fields := fieldSets[n-1]

			if _, err := i0.Get(fields); err != nil {
				b.Fatal(err)
			}
		}
	})

	err = i0.Close()
	if err != nil {
		b.Fatal(err)
	}
}
