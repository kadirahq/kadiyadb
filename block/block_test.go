package block

import (
	"fmt"
	"os"
	"reflect"
	"testing"
)

const (
	dir = "/tmp/sfile"
)

var (
	OptionsSet = map[string]*Options{
		"rw-mode": &Options{Path: dir, PSize: 1, RSize: 3, SSize: 5},
		"ro-mode": &Options{Path: dir, PSize: 1, RSize: 3, SSize: 5, ROnly: true},
	}
)

func TNewWithOptions(t *testing.T, o *Options) {
	err := os.RemoveAll(dir)
	if err != nil {
		t.Fatal(err)
	}

	b0, err := New(OptionsSet["rw-mode"])
	if err != nil {
		t.Fatal(err)
	}

	err = b0.Close()
	if err != nil {
		t.Fatal(err)
	}

	b0, err = New(o)
	if err != nil {
		t.Fatal(err)
	}

	err = b0.Close()
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

func TPutGetWithOptions(t *testing.T, o *Options) {
	err := os.RemoveAll(dir)
	if err != nil {
		t.Fatal(err)
	}

	b0, err := New(OptionsSet["rw-mode"])
	if err != nil {
		t.Fatal(err)
	}

	var i, j uint32
	for i = 0; i < 7; i++ {
		id, err := b0.Add()
		if err != nil {
			t.Fatal(err)
		}

		if i != id {
			t.Fatal("wrong id")
		}

		for j = 0; j < 3; j++ {
			p := []byte{byte(i*3 + j)}
			err = b0.Put(id, j, p)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	err = b0.Close()
	if err != nil {
		t.Fatal(err)
	}

	b0, err = New(o)
	if err != nil {
		t.Fatal(err)
	}

	for i = 0; i < 7; i++ {
		exp := make([][]byte, 3)
		for j = 0; j < 3; j++ {
			exp[j] = []byte{byte(i*3 + j)}
		}

		res, err := b0.Get(i, 0, 3)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(exp, res) {
			t.Fatal("wrong values")
		}
	}

	err = b0.Close()
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

func BAddWithSSize(b *testing.B, sz uint32) {
	b.ReportAllocs()

	if b.N > 10000 {
		b.N = 10000
	}

	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:  bpath,
		PSize: 10,
		RSize: 1000,
		SSize: 1000,
		ROnly: false,
	}

	options.SSize = sz

	blk, err := New(options)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		blk.Add()
	}

	err = blk.Close()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkAddSS1K(b *testing.B) { BAddWithSSize(b, 1000) }
func BenchmarkAddSS2K(b *testing.B) { BAddWithSSize(b, 2000) }
func BenchmarkAddSS5K(b *testing.B) { BAddWithSSize(b, 5000) }

func BPutWithPSize(b *testing.B, sz uint32) {
	b.ReportAllocs()

	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:  bpath,
		PSize: 10,
		RSize: 1000,
		SSize: 1000,
		ROnly: false,
	}

	options.PSize = sz

	blk, err := New(options)
	if err != nil {
		b.Fatal(err)
	}

	var i uint32
	for i = 0; i < options.SSize; i++ {
		id, err := blk.Add()
		if err != nil {
			b.Fatal(err)
		} else if id != i {
			b.Fatal("incorrect id")
		}
	}

	pld := make([]byte, options.PSize)
	N := uint32(b.N)

	b.ResetTimer()
	for i = 0; i < N; i++ {
		err = blk.Put(i%options.SSize, i%options.RSize, pld)
		if err != nil {
			b.Fatal(err)
		}
	}

	err = blk.Close()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkPutPS10(b *testing.B)   { BPutWithPSize(b, 10) }
func BenchmarkPutPS100(b *testing.B)  { BPutWithPSize(b, 100) }
func BenchmarkPutPS1000(b *testing.B) { BPutWithPSize(b, 1000) }

func BPutWithRSize(b *testing.B, sz uint32) {
	b.ReportAllocs()

	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:  bpath,
		PSize: 10,
		RSize: 1000,
		SSize: 1000,
		ROnly: false,
	}

	options.RSize = sz

	blk, err := New(options)
	if err != nil {
		b.Fatal(err)
	}

	var i uint32
	for i = 0; i < options.SSize; i++ {
		id, err := blk.Add()
		if err != nil {
			b.Fatal(err)
		} else if id != i {
			b.Fatal("incorrect id")
		}
	}

	pld := make([]byte, options.PSize)
	N := uint32(b.N)

	b.ResetTimer()
	for i = 0; i < N; i++ {
		err = blk.Put(i%options.SSize, i%options.RSize, pld)
		if err != nil {
			b.Fatal(err)
		}
	}

	err = blk.Close()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkPutRS100(b *testing.B)   { BPutWithRSize(b, 100) }
func BenchmarkPutRS1000(b *testing.B)  { BPutWithRSize(b, 1000) }
func BenchmarkPutRS10000(b *testing.B) { BPutWithRSize(b, 10000) }

func BPutWithSSize(b *testing.B, sz uint32) {
	b.ReportAllocs()

	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:  bpath,
		PSize: 10,
		RSize: 1000,
		SSize: 1000,
		ROnly: false,
	}

	options.SSize = sz

	blk, err := New(options)
	if err != nil {
		b.Fatal(err)
	}

	var i uint32
	for i = 0; i < options.SSize; i++ {
		id, err := blk.Add()
		if err != nil {
			b.Fatal(err)
		} else if id != i {
			b.Fatal("incorrect id")
		}
	}

	pld := make([]byte, options.PSize)
	N := uint32(b.N)

	b.ResetTimer()
	for i = 0; i < N; i++ {
		err = blk.Put(i%options.SSize, i%options.RSize, pld)
		if err != nil {
			b.Fatal(err)
		}
	}

	err = blk.Close()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkPutSS100(b *testing.B)   { BPutWithSSize(b, 100) }
func BenchmarkPutSS1000(b *testing.B)  { BPutWithSSize(b, 1000) }
func BenchmarkPutSS10000(b *testing.B) { BPutWithSSize(b, 10000) }

func BGetWithLen(b *testing.B, sz uint32) {
	b.ReportAllocs()

	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	// use a few segments
	var recordCount uint32 = 10000

	options := &Options{
		Path:  bpath,
		PSize: 10,
		RSize: 1000,
		SSize: 1000,
		ROnly: false,
	}

	options.RSize = sz

	blk, err := New(options)
	if err != nil {
		b.Fatal(err)
	}

	var i uint32
	for i = 0; i < recordCount; i++ {
		id, err := blk.Add()
		if err != nil {
			b.Fatal(err)
		} else if id != i {
			b.Fatal("incorrect id")
		}
	}

	N := uint32(b.N)
	b.ResetTimer()
	for i = 0; i < N; i++ {
		_, err = blk.Get(i%recordCount, 0, sz)
		if err != nil {
			b.Fatal(err)
		}
	}

	err = blk.Close()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkGetLen10(b *testing.B)   { BGetWithLen(b, 10) }
func BenchmarkGetLen100(b *testing.B)  { BGetWithLen(b, 100) }
func BenchmarkGetLen1000(b *testing.B) { BGetWithLen(b, 1000) }
