package index

import (
	"fmt"
	"os"
	"strconv"
	"testing"
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

func TNewWithOptions(t *testing.T, o *Options) {
	err := os.RemoveAll(dir)
	if err != nil {
		t.Fatal(err)
	}

	i0, err := New(OptionsSet["rw-mode"])
	if err != nil {
		t.Fatal(err)
	}

	err = i0.Close()
	if err != nil {
		t.Fatal(err)
	}

	i0, err = New(OptionsSet["ro-mode"])
	if err != nil {
		t.Fatal(err)
	}

	err = i0.Close()
	if err != nil {
		t.Fatal(err)
	}

	i0, err = New(o)
	if err != nil {
		t.Fatal(err)
	}

	err = i0.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = os.RemoveAll(dir)
	if err != nil {
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
		t.Fatal(err)
	}

	// always use rw-mode for writing data
	i0, err := New(OptionsSet["rw-mode"])
	if err != nil {
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c"}
	err = i0.Put(fields, 100)
	if err != nil {
		t.Fatal(err)
	}

	err = i0.Close()
	if err != nil {
		t.Fatal(err)
	}

	// open the index in ro-mode to create snapshot file
	// close is so it can be opened with user provided options
	i0, err = New(OptionsSet["ro-mode"])
	if err != nil {
		t.Fatal(err)
	}

	err = i0.Close()
	if err != nil {
		t.Fatal(err)
	}

	// finally, open with user provided options
	i0, err = New(o)
	if err != nil {
		t.Fatal(err)
	}

	item, err := i0.One(fields)
	if err != nil {
		t.Fatal(err)
	}

	if item.Value != 100 {
		t.Fatal("wrong value")
	}

	err = i0.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = os.RemoveAll(dir)
	if err != nil {
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
		t.Fatal(err)
	}

	// always use rw-mode for writing data
	i0, err := New(OptionsSet["rw-mode"])
	if err != nil {
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c"}
	err = i0.Put(fields, 100)
	if err != nil {
		t.Fatal(err)
	}

	err = i0.Close()
	if err != nil {
		t.Fatal(err)
	}

	// open the index in ro-mode to create snapshot file
	// close is so it can be opened with user provided options
	i0, err = New(OptionsSet["ro-mode"])
	if err != nil {
		t.Fatal(err)
	}

	err = i0.Close()
	if err != nil {
		t.Fatal(err)
	}

	// finally, open with user provided options
	i0, err = New(o)
	if err != nil {
		t.Fatal(err)
	}

	items, err := i0.Get(fields)
	if err != nil {
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
		t.Fatal(err)
	}

	err = os.RemoveAll(dir)
	if err != nil {
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

	var i int
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fields := fieldSets[i]
			i++

			err = i0.Put(fields, 100)
			if err != nil {
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

	var i int
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fields := fieldSets[i]
			i++

			_, err = i0.One(fields)
			if err != nil {
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

	var i int
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fields := fieldSets[i]
			i++

			_, err = i0.Get(fields)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	err = i0.Close()
	if err != nil {
		b.Fatal(err)
	}
}
