package mmap

import (
	"os"
	"reflect"
	"testing"
)

func TestNew(t *testing.T) {
	fpath := "/tmp/m1"
	defer os.Remove(fpath)

	m, err := New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	err = m.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestGrow(t *testing.T) {
	fpath := "/tmp/m1"
	defer os.Remove(fpath)

	m, err := New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	toGrow := int64(AllocChunkSize + 5)
	err = m.Grow(toGrow)
	if err != nil {
		t.Fatal(err)
	}

	if m.Size != toGrow {
		t.Fatal("incorrect mmap size")
	}
}

func TestLockUnlock(t *testing.T) {
	fpath := "/tmp/m1"
	defer os.Remove(fpath)

	m, err := New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	err = m.Lock()
	if err != nil {
		t.Fatal(err)
	}

	err = m.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteRead(t *testing.T) {
	fpath := "/tmp/m1"
	defer os.Remove(fpath)

	m, err := New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	err = m.Grow(4)
	if err != nil {
		t.Fatal(err)
	}

	testData := []byte{1, 2, 3, 4}
	n := copy(m.Data, testData)
	if n != 4 {
		t.Fatal("copy error")
	}

	err = m.Close()
	if err != nil {
		t.Fatal(err)
	}

	m2, err := New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(testData, m2.Data) {
		t.Fatal("incorrect data")
	}
}
