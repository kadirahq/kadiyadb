package kdb

import (
	"os/exec"
	"reflect"
	"testing"

	"github.com/meteorhacks/kdb/clock"
)

const (
	DatabasePath = "/tmp/d1"
)

var (
	DefaultOptions = &Options{
		Path:        DatabasePath,
		Resolution:  10,
		Duration:    1000,
		PayloadSize: 4,
		SegmentSize: 100,
		MaxROEpochs: 2,
		MaxRWEpochs: 2,
	}
)

func init() {
	clock.UseTestClock()
	clock.Goto(11999)
}

// A TEST CLOCK IS USED TO CONTROL THE TIME IN TESTS
// default read-write timestamp range:  10000 --- 12000
// anything below 10000 is loaded as read-only epochs
// anything above 11999 is the future

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

	db, err = Open(DatabasePath, false)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

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

	defer db.Close()
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
