package kadiyadb

import (
	"io/ioutil"
	"os/exec"
	"reflect"
	"testing"

	"github.com/kadirahq/kadiyadb/utils/clock"
)

const (
	DatabasePath = "/tmp/d1"
)

var (
	DefaultOptions = &Options{
		Path:        DatabasePath,
		Resolution:  10,
		Retention:   7000,
		Duration:    1000,
		PayloadSize: 4,
		SegmentSize: 100,
		MaxROEpochs: 2,
		MaxRWEpochs: 2,
	}
)

func init() {
	clock.UseTest()
	clock.Set(11999)
}

// A TEST CLOCK IS USED TO CONTROL THE TIME IN TESTS
// default future time range:      12000 --- .
// default read-write time range:  10000 --- 11999
// default read-only time range:    4000 ---  9999
// default expired time range:         0 ---  3999

func TestNew(t *testing.T) {
	exec.Command("rm", "-rf", DatabasePath).Run()

	db, err := New(DefaultOptions)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()
}

func TestOpen(t *testing.T) {
	exec.Command("rm", "-rf", DatabasePath).Run()

	db, err := New(DefaultOptions)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	// recovery mode set to false
	db, err = Open(DatabasePath, false)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	// recovery mode set to true
	db, err = Open(DatabasePath, true)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()
}

func TestEditMetadata(t *testing.T) {
	exec.Command("rm", "-rf", DatabasePath).Run()

	db, err := New(DefaultOptions)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	d := db.(*database)
	realMaxROEpochs := d.metadata.MaxROEpochs
	d.metadata.MaxROEpochs = 9999

	err = d.mdstore.Load()
	if err != nil {
		t.Fatal(err)
	}

	if d.metadata.MaxROEpochs != realMaxROEpochs {
		t.Fatal("load failed")
	}

	err = d.Edit(&Metadata{
		MaxROEpochs: 3,
		MaxRWEpochs: 3,
	})

	if err != nil {
		t.Fatal(err)
	}

	if d.metadata.MaxROEpochs != 3 {
		t.Fatal("edit failed")
	}
}

func TestPutGet(t *testing.T) {
	exec.Command("rm", "-rf", DatabasePath).Run()

	db, err := New(DefaultOptions)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	fields := []string{"a", "b", "c", "d"}
	value1 := []byte{1, 2, 3, 4}
	value2 := []byte{5, 6, 7, 8}

	err = db.Put(10990, fields, value1)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put(11000, fields, value2)
	if err != nil {
		t.Fatal(err)
	}

	res, err := db.Get(10990, 11010, fields)
	if err != nil {
		t.Fatal(err)
	}

	// res only has one result
	for item, points := range res {
		if !reflect.DeepEqual(item.Fields, fields) ||
			!reflect.DeepEqual([][]byte{value1, value2}, points) {
			t.Fatal("incorrect results")
		}
	}
}

func TestPutOldData(t *testing.T) {
	exec.Command("rm", "-rf", DatabasePath).Run()

	db, err := New(DefaultOptions)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	fields := []string{"a", "b", "c", "d"}
	value1 := []byte{1, 2, 3, 4}

	err = db.Put(9990, fields, value1)
	if err == nil {
		t.Fatal("should return epoch not found error")
	}
}

func TestPutWithRec(t *testing.T) {
	exec.Command("rm", "-rf", DatabasePath).Run()

	options := *DefaultOptions
	options.Recovery = true

	db, err := New(&options)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	fields := []string{"a", "b", "c", "d"}
	value1 := []byte{1, 2, 3, 4}

	err = db.Put(9990, fields, value1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestExpireOldData(t *testing.T) {
	exec.Command("rm", "-rf", DatabasePath).Run()

	db, err := New(DefaultOptions)
	if err != nil {
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c", "d"}
	value := []byte{1, 2, 3, 4}

	clock.Set(5999)
	err = db.Put(4999, fields, value)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put(5999, fields, value)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	files, err := ioutil.ReadDir(DatabasePath)
	if err != nil {
		t.Fatal(err)
	}

	if len(files) != 3 {
		t.Fatal("incorrect number of files")
	}

	clock.Set(11999)
	db, err = Open(DatabasePath, false)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	out1, err := db.One(4990, 5000, fields)
	if err != nil {
		t.Fatal(err)
	}

	if len(out1[0]) != 0 {
		t.Fatal("expired data should not exist")
	}

	out2, err := db.One(5990, 6000, fields)
	if err != nil {
		t.Fatal(err)
	}

	if len(out2[0]) != 4 {
		t.Fatal("data should exist", out2)
	}
}

func BenchmarkPut(b *testing.B) {
	exec.Command("rm", "-rf", DatabasePath).Run()

	db, err := New(DefaultOptions)
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	value := []byte{1, 2, 3, 4}
	fields := []string{"a", "b", "c", "d"}

	var i int64
	var N = int64(b.N)

	b.ResetTimer()
	for i = 0; i < N; i++ {
		ts := 11000 + (10*i)%1000
		err = db.Put(ts, fields, value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	exec.Command("rm", "-rf", DatabasePath).Run()

	db, err := New(DefaultOptions)
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	value := []byte{1, 2, 3, 4}
	fields := []string{"a", "b", "c", "d"}

	var i int64
	var N = int64(b.N)

	for i = 0; i < N; i++ {
		ts := 11000 + (10*i)%1000
		err = db.Put(ts, fields, value)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for i = 0; i < N; i++ {
		ts := 11000 + (10*i)%990
		_, err = db.Get(ts, ts+10, fields)
		if err != nil {
			b.Fatal(err)
		}
	}
}
