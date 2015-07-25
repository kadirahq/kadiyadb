package block

import (
	"os"
	"testing"
)

func TestNewRO(t *testing.T) {
	bpath := "/tmp/b1"
	os.MkdirAll(bpath, 0755)
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:  bpath,
		PSize: 1,
		RSize: 3,
		SSize: 5,
		ROnly: true,
	}

	b, err := New(options)
	if err != nil {
		t.Fatal(err)
	}

	rb, ok := b.(*roblock)
	if !ok {
		t.Fatal("incorrect block type")
	}

	if rb.mdstore != nil {
		t.Fatal("ro blocks shouldn't have an mdstore")
	}

	err = b.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewRW(t *testing.T) {
	bpath := "/tmp/b1"
	os.MkdirAll(bpath, 0755)
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

	_, ok := b.(*rwblock)
	if !ok {
		t.Fatal("incorrect block type")
	}

	err = b.Close()
	if err != nil {
		t.Fatal(err)
	}
}
