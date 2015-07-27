package block

import (
	"os"
	"reflect"
	"testing"
)

func TestROAdd(t *testing.T) {
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

	_, err = b.Add()
	if err != ErrROnly {
		t.Fatal("should return error")
	}

	err = b.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestROPut(t *testing.T) {
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

	err = b.Put(0, 0, []byte{1, 2, 3})
	if err != ErrROnly {
		t.Fatal("should return error")
	}

	err = b.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestROGet(t *testing.T) {
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

	var i uint32
	for i = 0; i < 7; i++ {
		id, err := b.Add()
		if err != nil {
			t.Fatal(err)
		} else if id != i {
			t.Fatal("incorrect id")
		}
	}

	pld := []byte{5}
	err = b.Put(0, 0, pld)
	if err != nil {
		t.Fatal(err)
	}
	err = b.Put(0, 1, pld)
	if err != nil {
		t.Fatal(err)
	}
	err = b.Put(1, 0, pld)
	if err != nil {
		t.Fatal(err)
	}

	err = b.Close()
	if err != nil {
		t.Fatal(err)
	}

	options.ROnly = true
	b2, err := New(options)
	if err != nil {
		t.Fatal(err)
	}

	res, err := b2.Get(0, 0, 2)
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{[]byte{5}, []byte{5}}
	if !reflect.DeepEqual(exp, res) {
		t.Fatal("invalid data")
	}

	err = b2.Close()
	if err != nil {
		t.Fatal(err)
	}
}
