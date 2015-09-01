package kadiyadb

import (
	"os"
	"reflect"
	"testing"

	"github.com/kadirahq/go-tools/logger"
)

func init() {
	logger.Disable("time")
}

func TestNewEpoch(t *testing.T) {
	bpath := "/tmp/t1"
	defer os.RemoveAll(bpath)

	options := &EpochOptions{
		Path:  bpath,
		PSize: 1,
		RSize: 3,
		SSize: 5,
		ROnly: false,
	}

	epo, err := NewEpoch(options)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	err = epo.Close()
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}

func TestOpenEpoch(t *testing.T) {
	bpath := "/tmp/t1"
	defer os.RemoveAll(bpath)

	options := &EpochOptions{
		Path:  bpath,
		PSize: 1,
		RSize: 3,
		SSize: 5,
		ROnly: false,
	}

	epo, err := NewEpoch(options)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	err = epo.Close()
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	options.ROnly = true
	ep2, err := NewEpoch(options)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	err = ep2.Close()
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}

func TestEpochPut(t *testing.T) {
	bpath := "/tmp/t1"
	defer os.RemoveAll(bpath)

	options := &EpochOptions{
		Path:  bpath,
		PSize: 1,
		RSize: 3,
		SSize: 5,
		ROnly: false,
	}

	epo, err := NewEpoch(options)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c"}
	value := []byte{5}
	err = epo.Put(0, fields, value)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	tt := epo.(*epoch)
	indexItem, err := tt.index.One(fields)
	if !reflect.DeepEqual(indexItem.Fields, fields) {
		t.Fatal("incorrect fields on index")
	}

	out, err := tt.block.Get(indexItem.Value, 0, 1)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if !reflect.DeepEqual(out, [][]byte{[]byte{5}}) {
		t.Fatal("incorrect fields on index")
	}

	err = epo.Close()
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}

func TestEpochGet(t *testing.T) {
	bpath := "/tmp/t1"
	defer os.RemoveAll(bpath)

	options := &EpochOptions{
		Path:  bpath,
		PSize: 1,
		RSize: 3,
		SSize: 5,
		ROnly: false,
	}

	epo, err := NewEpoch(options)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c"}
	value := []byte{5}
	err = epo.Put(0, fields, value)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	out, err := epo.Get(0, 1, fields)
	if err != nil {
		logger.Error(err)
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
		logger.Error(err)
		t.Fatal(err)
	}
}
