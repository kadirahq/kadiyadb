package block

import (
	"os"
	"testing"
)

func TestNewROBlock(t *testing.T) {
	bpath := "/tmp/b1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:          bpath,
		PayloadSize:   1,
		PayloadCount:  3,
		SegmentLength: 5,
		ReadOnly:      true,
	}

	b, err := New(options)
	if err != nil {
		t.Fatal(err)
	}

	bb := b.(*roblock)
	err = bb.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestROAdd(t *testing.T) {

}

func TestROPut(t *testing.T) {

}

func TestROGet(t *testing.T) {

}
