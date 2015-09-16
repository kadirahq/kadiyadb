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
	for i := 1; i <= len(fields); i++ {
		flds := fields[:i]

		indexItem, err := tt.index.One(flds)
		if !reflect.DeepEqual(indexItem.Fields, flds) {
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
	}

	err = epo.Close()
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}

func TestEpochGetOne(t *testing.T) {
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

	f1 := []string{"a", "b", "c"}
	v1 := []byte{5}

	err = epo.Put(0, f1, v1)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	// first make sure we've added data to sub levels
	// {"a"}, {"a", "b"} should also have []byte{5}
	for i := 1; i <= len(f1); i++ {
		flds := f1[:i]

		out, err := epo.Get(0, 1, flds)
		if err != nil {
			logger.Error(err)
			t.Fatal(err)
		}

		if len(out) != 1 {
			t.Fatal("incorrect number of result items")
		}

		for item, res := range out {
			if !reflect.DeepEqual(item.Fields, flds) {
				t.Fatal("incorrect fields on index")
			}

			if !reflect.DeepEqual(res, [][]byte{[]byte{5}}) {
				t.Fatal("incorrect fields on index")
			}
		}
	}

	// add some more data for more complex queries
	if err := epo.Put(0, []string{"a", "b", "d"}, []byte{6}); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := epo.Put(0, []string{"a", "c", "d"}, []byte{7}); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	// try getting {"a", "b"} which should have value from {"a", "b", "d"}
	// after the update. It used to have the value from {"a". "b", "c"}
	{
		out, err := epo.Get(0, 1, []string{"a", "b"})
		if err != nil {
			logger.Error(err)
			t.Fatal(err)
		}

		if len(out) != 1 {
			t.Fatal("incorrect number of result items")
		}

		for item, res := range out {
			if !reflect.DeepEqual(item.Fields, []string{"a", "b"}) {
				t.Fatal("incorrect fields on index")
			}

			if !reflect.DeepEqual(res, [][]byte{[]byte{6}}) {
				t.Fatal("incorrect fields on index")
			}
		}
	}

	// try getting {"a", "b", ""} which should have value from both entries
	// {"a", "b", "c"} and {"a", "b", "c"}. Differs from {"a", "b"} result
	{
		out, err := epo.Get(0, 1, []string{"a", "b", ""})
		if err != nil {
			logger.Error(err)
			t.Fatal(err)
		}

		if len(out) != 2 {
			t.Fatal("incorrect number of result items")
		}

		for item, res := range out {
			if reflect.DeepEqual(item.Fields, []string{"a", "b", "c"}) {
				if !reflect.DeepEqual(res, [][]byte{[]byte{5}}) {
					t.Fatal("incorrect fields on index")
				}
			} else if reflect.DeepEqual(item.Fields, []string{"a", "b", "d"}) {
				if !reflect.DeepEqual(res, [][]byte{[]byte{6}}) {
					t.Fatal("incorrect fields on index")
				}
			} else {
				t.Fatal("incorrect fields on index")
			}
		}
	}

	// try getting {"a", "", "d"} which should have value from both entries
	// {"a", "b", "d"} and {"a", "c", "d"}.
	{
		out, err := epo.Get(0, 1, []string{"a", "", "d"})
		if err != nil {
			logger.Error(err)
			t.Fatal(err)
		}

		if len(out) != 2 {
			t.Fatal("incorrect number of result items")
		}

		for item, res := range out {
			if reflect.DeepEqual(item.Fields, []string{"a", "b", "d"}) {
				if !reflect.DeepEqual(res, [][]byte{[]byte{6}}) {
					t.Fatal("incorrect fields on index")
				}
			} else if reflect.DeepEqual(item.Fields, []string{"a", "c", "d"}) {
				if !reflect.DeepEqual(res, [][]byte{[]byte{7}}) {
					t.Fatal("incorrect fields on index")
				}
			} else {
				t.Fatal("incorrect fields on index")
			}
		}
	}

	if err := epo.Close(); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}
