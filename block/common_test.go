package block

import (
	"os"
	"reflect"
	"testing"
)

func TestSaveMetadata(t *testing.T) {
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
	err = bb.saveMetadata()
	if err != nil {
		t.Fatal(err)
	}

	err = bb.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoadMetadata(t *testing.T) {
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
	md := &Metadata{
		RecordCount:   300,
		SegmentCount:  200,
		SegmentLength: 100,
	}

	bb.metadata = md
	err = bb.saveMetadata()
	if err != nil {
		t.Fatal(err)
	}

	bb.metadata = &Metadata{}
	err = bb.loadMetadata()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(*md, *bb.metadata) {
		t.Fatal("invalid values")
	}

	err = bb.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkSaveMetadata(b *testing.B) {
	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:          bpath,
		PayloadSize:   1,
		PayloadCount:  3,
		SegmentLength: 5,
		ReadOnly:      false,
	}

	blk, err := New(options)
	if err != nil {
		b.Fatal(err)
	}

	bb := blk.(*rwblock)
	bb.metadata = &Metadata{
		RecordCount:   300,
		SegmentCount:  200,
		SegmentLength: 100,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = bb.saveMetadata()
		if err != nil {
			b.Fatal(err)
		}
	}

	err = bb.Close()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkLoadMetadata(b *testing.B) {
	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:          bpath,
		PayloadSize:   1,
		PayloadCount:  3,
		SegmentLength: 5,
		ReadOnly:      false,
	}

	blk, err := New(options)
	if err != nil {
		b.Fatal(err)
	}

	bb := blk.(*rwblock)
	bb.metadata = &Metadata{
		RecordCount:   300,
		SegmentCount:  200,
		SegmentLength: 100,
	}

	err = bb.saveMetadata()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = bb.loadMetadata()
		if err != nil {
			b.Fatal(err)
		}
	}

	err = bb.Close()
	if err != nil {
		b.Fatal(err)
	}
}
