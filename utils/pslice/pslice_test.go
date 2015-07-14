package pslice

import (
	"os"
	"testing"
)

func TestNewSliceNewFile(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	s, err := New(&Options{Path: fpath, Size: 3})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewSliceOldFile(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	_, err := os.Create(fpath)
	if err != nil {
		t.Fatal(err)
	}

	_, err = New(&Options{Path: fpath, Size: 3})
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewSliceEmpty(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	_, err := New(&Options{Path: fpath, Size: 0})
	if err != ErrEmpty {
		t.Fatal("should return an error")
	}
}

func TestWriteRead(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	s, err := New(&Options{Path: fpath, Size: 3})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Put(0, 10)
	if err != nil {
		t.Fatal(err)
	}

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}

	s2, err := New(&Options{Path: fpath, Size: 3})
	if err != nil {
		t.Fatal(err)
	}

	el, err := s2.Get(0)
	if err != nil {
		t.Fatal(err)
	} else if el != 10 {
		t.Fatal("incorrect value")
	}
}

func BenchWriteSize(b *testing.B, size int64) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	s, err := New(&Options{Path: fpath, Size: size})
	if err != nil {
		b.Fatal(err)
	}

	var i int64
	var N = int64(b.N)
	b.ResetTimer()

	for i = 0; i < N; i++ {
		err = s.Put(i%size, i)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteSize10(b *testing.B)   { BenchWriteSize(b, 10) }
func BenchmarkWriteSize100(b *testing.B)  { BenchWriteSize(b, 100) }
func BenchmarkWriteSize1000(b *testing.B) { BenchWriteSize(b, 1000) }

func BenchReadSize(b *testing.B, size int64) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	s, err := New(&Options{Path: fpath, Size: size})
	if err != nil {
		b.Fatal(err)
	}

	var i int64
	var N = int64(b.N)

	for i = 0; i < N; i++ {
		err = s.Put(i%size, i)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i = 0; i < N; i++ {
		_, err = s.Get(i % size)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSize10(b *testing.B)   { BenchReadSize(b, 10) }
func BenchmarkReadSize100(b *testing.B)  { BenchReadSize(b, 100) }
func BenchmarkReadSize1000(b *testing.B) { BenchReadSize(b, 1000) }
