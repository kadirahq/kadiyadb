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
		Path:          bpath,
		PayloadSize:   1,
		PayloadCount:  3,
		SegmentLength: 5,
		ReadOnly:      false,
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
		Path:          bpath,
		PayloadSize:   1,
		PayloadCount:  3,
		SegmentLength: 5,
		ReadOnly:      false,
	}

	b, err := New(options)
	if err != nil {
		t.Fatal(err)
	}

	bb := b.(*rwblock)

	var i int64
	for i = 0; i < 7; i++ {
		id, err := bb.Add()
		if err != nil {
			t.Fatal(err)
		} else if id != i {
			t.Fatal("incorrect id")
		}
	}

	if bb.metadata.RecordCount != 7 {
		t.Fatal("incorrect number of records")
	}

	if bb.metadata.SegmentCount != 2 {
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
		Path:          bpath,
		PayloadSize:   1,
		PayloadCount:  3,
		SegmentLength: 5,
		ReadOnly:      false,
	}

	b, err := New(options)
	if err != nil {
		t.Fatal(err)
	}

	bb := b.(*rwblock)

	var i int64
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

	for i := 0; i < 2; i++ {
		if !reflect.DeepEqual(expSegs[i], bb.segments[i].Data) {
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
		Path:          bpath,
		PayloadSize:   1,
		PayloadCount:  3,
		SegmentLength: 5,
		ReadOnly:      false,
	}

	b, err := New(options)
	if err != nil {
		t.Fatal(err)
	}

	bb := b.(*rwblock)

	var i int64
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

func BenchRWAddRC(b *testing.B, sz int64) {
	b.ReportAllocs()

	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:          bpath,
		PayloadSize:   10,
		PayloadCount:  100,
		SegmentLength: 1000,
		ReadOnly:      false,
	}

	options.SegmentLength = sz

	blk, err := New(options)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		// reset every 10k
		if i%10000 == 0 && i != 0 {
			b.StopTimer()

			if blk != nil {
				err = blk.Close()
				if err != nil {
					b.Fatal(err)
				}

				err = os.RemoveAll(bpath)
				if err != nil {
					err = os.RemoveAll(bpath)
					if err != nil {
						b.Error(err)
					}
				}
			}

			blk, err = New(options)
			if err != nil {
				b.Fatal(err)
			}

			b.StartTimer()
		}

		// benchmark add
		_, err = blk.Add()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRWAddRC1K(b *testing.B)   { BenchRWAddRC(b, 1000) }
func BenchmarkRWAddRC10K(b *testing.B)  { BenchRWAddRC(b, 10000) }
func BenchmarkRWAddRC100K(b *testing.B) { BenchRWAddRC(b, 100000) }

func BenchRWPutPS(b *testing.B, sz int64) {
	b.ReportAllocs()

	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:          bpath,
		PayloadSize:   10,
		PayloadCount:  100,
		SegmentLength: 1000,
		ReadOnly:      false,
	}

	options.PayloadSize = sz

	blk, err := New(options)
	if err != nil {
		b.Fatal(err)
	}

	bb := blk.(*rwblock)

	var i int64
	for i = 0; i < options.SegmentLength; i++ {
		id, err := bb.Add()
		if err != nil {
			b.Fatal(err)
		} else if id != i {
			b.Fatal("incorrect id")
		}
	}

	pld := make([]byte, options.PayloadSize)
	N := int64(b.N)

	b.ResetTimer()
	for i = 0; i < N; i++ {
		err = bb.Put(i%options.SegmentLength, i%options.PayloadCount, pld)
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

func BenchRWPutPC(b *testing.B, sz int64) {
	b.ReportAllocs()

	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:          bpath,
		PayloadSize:   10,
		PayloadCount:  100,
		SegmentLength: 1000,
		ReadOnly:      false,
	}

	options.PayloadCount = sz

	blk, err := New(options)
	if err != nil {
		b.Fatal(err)
	}

	bb := blk.(*rwblock)

	var i int64
	for i = 0; i < options.SegmentLength; i++ {
		id, err := bb.Add()
		if err != nil {
			b.Fatal(err)
		} else if id != i {
			b.Fatal("incorrect id")
		}
	}

	pld := make([]byte, options.PayloadSize)
	N := int64(b.N)

	b.ResetTimer()
	for i = 0; i < N; i++ {
		err = bb.Put(i%options.SegmentLength, i%options.PayloadCount, pld)
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

func BenchRWPutRC(b *testing.B, sz int64) {
	b.ReportAllocs()

	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:          bpath,
		PayloadSize:   10,
		PayloadCount:  100,
		SegmentLength: 1000,
		ReadOnly:      false,
	}

	options.SegmentLength = sz

	blk, err := New(options)
	if err != nil {
		b.Fatal(err)
	}

	bb := blk.(*rwblock)

	var i int64
	for i = 0; i < options.SegmentLength; i++ {
		id, err := bb.Add()
		if err != nil {
			b.Fatal(err)
		} else if id != i {
			b.Fatal("incorrect id")
		}
	}

	pld := make([]byte, options.PayloadSize)
	N := int64(b.N)

	b.ResetTimer()
	for i = 0; i < N; i++ {
		err = bb.Put(i%options.SegmentLength, i%options.PayloadCount, pld)
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

func BenchRWGetLen(b *testing.B, sz int64) {
	b.ReportAllocs()

	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	// use a few segments
	var recordCount int64 = 10000

	options := &Options{
		Path:          bpath,
		PayloadSize:   10,
		PayloadCount:  100,
		SegmentLength: 1000,
		ReadOnly:      false,
	}

	options.PayloadCount = sz

	blk, err := New(options)
	if err != nil {
		b.Fatal(err)
	}

	bb := blk.(*rwblock)

	var i int64
	for i = 0; i < recordCount; i++ {
		id, err := bb.Add()
		if err != nil {
			b.Fatal(err)
		} else if id != i {
			b.Fatal("incorrect id")
		}
	}

	N := int64(b.N)
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
