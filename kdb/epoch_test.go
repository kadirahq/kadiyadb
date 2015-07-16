package kdb

import (
	"os"
	"reflect"
	"testing"
)

func TestNewEpoch(t *testing.T) {
	bpath := "/tmp/t1"
	defer os.RemoveAll(bpath)

	options := &EpochOptions{
		Path:          bpath,
		PayloadSize:   1,
		PayloadCount:  3,
		SegmentLength: 5,
		ReadOnly:      false,
	}

	epo, err := NewEpoch(options)
	if err != nil {
		t.Fatal(err)
	}

	err = epo.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestOpen(t *testing.T) {
	bpath := "/tmp/t1"
	defer os.RemoveAll(bpath)

	options := &EpochOptions{
		Path:          bpath,
		PayloadSize:   1,
		PayloadCount:  3,
		SegmentLength: 5,
		ReadOnly:      false,
	}

	epo, err := NewEpoch(options)
	if err != nil {
		t.Fatal(err)
	}

	err = epo.Close()
	if err != nil {
		t.Fatal(err)
	}

	options.ReadOnly = true
	ep2, err := NewEpoch(options)
	if err != nil {
		t.Fatal(err)
	}

	err = ep2.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestPut(t *testing.T) {
	bpath := "/tmp/t1"
	defer os.RemoveAll(bpath)

	options := &EpochOptions{
		Path:          bpath,
		PayloadSize:   1,
		PayloadCount:  3,
		SegmentLength: 5,
		ReadOnly:      false,
	}

	epo, err := NewEpoch(options)
	if err != nil {
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c"}
	value := []byte{5}
	err = epo.Put(0, fields, value)
	if err != nil {
		t.Fatal(err)
	}

	tt := epo.(*epoch)
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

	err = epo.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestGet(t *testing.T) {
	bpath := "/tmp/t1"
	defer os.RemoveAll(bpath)

	options := &EpochOptions{
		Path:          bpath,
		PayloadSize:   1,
		PayloadCount:  3,
		SegmentLength: 5,
		ReadOnly:      false,
	}

	epo, err := NewEpoch(options)
	if err != nil {
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c"}
	value := []byte{5}
	err = epo.Put(0, fields, value)
	if err != nil {
		t.Fatal(err)
	}

	out, err := epo.Get(0, 1, fields)
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

	err = epo.Close()
	if err != nil {
		t.Fatal(err)
	}
}
