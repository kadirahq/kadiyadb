package block

import (
	"os"
	"reflect"
	"testing"
)

func TestNewRWBlock(t *testing.T) {
	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:  bpath,
		PSize: 1,
		RSize: 3,
		SSize: 5,
		ROnly: false,
	}

	b, err := New(options)
	if err != nil {
		t.Fatal(err)
	}

	bb := b.(*rwblock)
	err = bb.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestRWAdd(t *testing.T) {
	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:  bpath,
		PSize: 1,
		RSize: 3,
		SSize: 5,
		ROnly: false,
	}

	b, err := New(options)
	if err != nil {
		t.Fatal(err)
	}

	bb := b.(*rwblock)

	var i uint32
	for i = 0; i < 7; i++ {
		id, err := bb.Add()
		if err != nil {
			t.Fatal(err)
		} else if id != i {
			t.Fatal("incorrect id")
		}
	}

	if bb.metadata.Records != 7 {
		t.Fatal("incorrect number of records")
	}

	if bb.metadata.Segments != 2 {
		t.Fatal("incorrect number of segments")
	}

	err = bb.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestRWPut(t *testing.T) {
	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:  bpath,
		PSize: 1,
		RSize: 3,
		SSize: 5,
		ROnly: false,
	}

	b, err := New(options)
	if err != nil {
		t.Fatal(err)
	}

	bb := b.(*rwblock)

	var i uint32
	for i = 0; i < 7; i++ {
		id, err := bb.Add()
		if err != nil {
			t.Fatal(err)
		} else if id != i {
			t.Fatal("incorrect id")
		}
	}

	pld := []byte{5}
	err = bb.Put(0, 0, pld)
	if err != nil {
		t.Fatal(err)
	}
	err = bb.Put(0, 1, pld)
	if err != nil {
		t.Fatal(err)
	}
	err = bb.Put(1, 0, pld)
	if err != nil {
		t.Fatal(err)
	}
	err = bb.Put(5, 0, pld)
	if err != nil {
		t.Fatal(err)
	}

	expSegs := [][]byte{
		[]byte{5, 5, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	}

	segSize := options.PSize * options.RSize * options.SSize

	for i := 0; i < 2; i++ {
		segData := make([]byte, segSize)
		n, err := bb.segments[i].ReadAt(segData, 0)
		if err != nil {
			t.Fatal(err)
		} else if uint32(n) != segSize {
			t.Fatal("read error")
		}

		if !reflect.DeepEqual(expSegs[i], segData) {
			t.Fatal("invalid data")
		}
	}

	err = bb.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestRWGet(t *testing.T) {
	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:  bpath,
		PSize: 1,
		RSize: 3,
		SSize: 5,
		ROnly: false,
	}

	b, err := New(options)
	if err != nil {
		t.Fatal(err)
	}

	bb := b.(*rwblock)

	var i uint32
	for i = 0; i < 7; i++ {
		id, err := bb.Add()
		if err != nil {
			t.Fatal(err)
		} else if id != i {
			t.Fatal("incorrect id")
		}
	}

	pld := []byte{5}
	err = bb.Put(0, 0, pld)
	if err != nil {
		t.Fatal(err)
	}
	err = bb.Put(0, 1, pld)
	if err != nil {
		t.Fatal(err)
	}
	err = bb.Put(1, 0, pld)
	if err != nil {
		t.Fatal(err)
	}

	res, err := bb.Get(0, 0, 2)
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{[]byte{5}, []byte{5}}
	if !reflect.DeepEqual(exp, res) {
		t.Fatal("invalid data")
	}

	err = bb.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func BenchRWAddSL(b *testing.B, sz uint32) {
	b.ReportAllocs()
	b.N = 50000

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

func BenchmarkRWAddSL1K(b *testing.B)  { BenchRWAddSL(b, 1000) }
func BenchmarkRWAddSL2K(b *testing.B)  { BenchRWAddSL(b, 2000) }
func BenchmarkRWAddSL10K(b *testing.B) { BenchRWAddSL(b, 10000) }

func BenchRWPutPS(b *testing.B, sz uint32) {
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

	bb := blk.(*rwblock)

	var i uint32
	for i = 0; i < options.SSize; i++ {
		id, err := bb.Add()
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
		err = bb.Put(i%options.SSize, i%options.RSize, pld)
		if err != nil {
			b.Fatal(err)
		}
	}

	err = bb.Close()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkRWPutPS10(b *testing.B)   { BenchRWPutPS(b, 10) }
func BenchmarkRWPutPS100(b *testing.B)  { BenchRWPutPS(b, 100) }
func BenchmarkRWPutPS1000(b *testing.B) { BenchRWPutPS(b, 1000) }

func BenchRWPutPC(b *testing.B, sz uint32) {
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

	bb := blk.(*rwblock)

	var i uint32
	for i = 0; i < options.SSize; i++ {
		id, err := bb.Add()
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
		err = bb.Put(i%options.SSize, i%options.RSize, pld)
		if err != nil {
			b.Fatal(err)
		}
	}

	err = bb.Close()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkRWPutPC100(b *testing.B)   { BenchRWPutPC(b, 100) }
func BenchmarkRWPutPC1000(b *testing.B)  { BenchRWPutPC(b, 1000) }
func BenchmarkRWPutPC10000(b *testing.B) { BenchRWPutPC(b, 10000) }

func BenchRWPutRC(b *testing.B, sz uint32) {
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

	bb := blk.(*rwblock)

	var i uint32
	for i = 0; i < options.SSize; i++ {
		id, err := bb.Add()
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
		err = bb.Put(i%options.SSize, i%options.RSize, pld)
		if err != nil {
			b.Fatal(err)
		}
	}

	err = bb.Close()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkRWPutRC100(b *testing.B)   { BenchRWPutRC(b, 100) }
func BenchmarkRWPutRC1000(b *testing.B)  { BenchRWPutRC(b, 1000) }
func BenchmarkRWPutRC10000(b *testing.B) { BenchRWPutRC(b, 10000) }

func BenchRWGetLen(b *testing.B, sz uint32) {
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

	bb := blk.(*rwblock)

	var i uint32
	for i = 0; i < recordCount; i++ {
		id, err := bb.Add()
		if err != nil {
			b.Fatal(err)
		} else if id != i {
			b.Fatal("incorrect id")
		}
	}

	N := uint32(b.N)
	b.ResetTimer()
	for i = 0; i < N; i++ {
		_, err = bb.Get(i%recordCount, 0, sz)
		if err != nil {
			b.Fatal(err)
		}
	}

	err = bb.Close()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkRWGetLen10(b *testing.B)   { BenchRWGetLen(b, 10) }
func BenchmarkRWGetLen100(b *testing.B)  { BenchRWGetLen(b, 100) }
func BenchmarkRWGetLen1000(b *testing.B) { BenchRWGetLen(b, 1000) }
