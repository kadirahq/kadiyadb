package mmap

import (
	"fmt"
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

	if m.Size() != toGrow {
		t.Fatal("incorrect mmap size")
	}
}

func TestAutoGrow(t *testing.T) {
	fpath := "/tmp/m1"
	defer os.Remove(fpath)

	toGrow := int64(AllocChunkSize + 5)
	m, err := New(&Options{Path: fpath, Size: toGrow})
	if err != nil {
		t.Fatal(err)
	}

	if m.Size() != toGrow {
		t.Fatal("incorrect mmap size", m.Size(), toGrow)
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
	n, err := m.WriteAt(testData, 0)
	if err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal("write error")
	}

	err = m.Close()
	if err != nil {
		t.Fatal(err)
	}

	m2, err := New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	readData := make([]byte, 4)
	n, err = m2.ReadAt(readData, 0)
	if err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal("read error")
	}

	if !reflect.DeepEqual(testData, readData) {
		fmt.Println(testData, readData)
		t.Fatal("incorrect data")
	}
}
