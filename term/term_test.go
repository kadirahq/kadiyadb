package term

import (
	"os"
	"reflect"
	"testing"
)

func TestNew(t *testing.T) {
	bpath := "/tmp/t1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:          bpath,
		PayloadSize:   1,
		PayloadCount:  3,
		SegmentLength: 5,
		ReadOnly:      false,
	}

	trm, err := New(options)
	if err != nil {
		t.Fatal(err)
	}

	err = trm.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestOpen(t *testing.T) {
	bpath := "/tmp/t1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:          bpath,
		PayloadSize:   1,
		PayloadCount:  3,
		SegmentLength: 5,
		ReadOnly:      false,
	}

	trm, err := New(options)
	if err != nil {
		t.Fatal(err)
	}

	err = trm.Close()
	if err != nil {
		t.Fatal(err)
	}

	trm2, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}

	err = trm2.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestPut(t *testing.T) {
	bpath := "/tmp/t1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:          bpath,
		PayloadSize:   1,
		PayloadCount:  3,
		SegmentLength: 5,
		ReadOnly:      false,
	}

	trm, err := New(options)
	if err != nil {
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c"}
	value := []byte{5}
	err = trm.Put(0, fields, value)
	if err != nil {
		t.Fatal(err)
	}

	tt := trm.(*term)
	indexItem, err := tt.idx.One(fields)
	if !reflect.DeepEqual(indexItem.Fields, fields) {
		t.Fatal("incorrect fields on index")
	}

	id, err := decodeInt64(indexItem.Value)
	if err != nil {
		t.Fatal(err)
	}

	out, err := tt.blk.Get(id, 0, 1)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(out, [][]byte{[]byte{5}}) {
		t.Fatal("incorrect fields on index")
	}

	err = trm.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestGet(t *testing.T) {
	bpath := "/tmp/t1"
	defer os.RemoveAll(bpath)

	options := &Options{
		Path:          bpath,
		PayloadSize:   1,
		PayloadCount:  3,
		SegmentLength: 5,
		ReadOnly:      false,
	}

	trm, err := New(options)
	if err != nil {
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c"}
	value := []byte{5}
	err = trm.Put(0, fields, value)
	if err != nil {
		t.Fatal(err)
	}

	out, err := trm.Get(0, 1, fields)
	if err != nil {
		t.Fatal(err)
	}

	if len(out) != 1 {
		t.Fatal("incorrect number of result items")
	}

	for item, res := range out {
		if !reflect.DeepEqual(item.Fields, fields) {
			t.Fatal("incorrect fields on index")
		}

		if !reflect.DeepEqual(res, [][]byte{[]byte{5}}) {
			t.Fatal("incorrect fields on index")
		}
	}

	err = trm.Close()
	if err != nil {
		t.Fatal(err)
	}
}
