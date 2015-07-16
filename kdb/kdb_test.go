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

// A TEST CLOCK IS USED TO CONTROL THE TIME
// read-write timestamp range:  10000 --- 12000
// anything below 10000 is loaded as read-only terms
// read-only data available at 3030 and 6060 timestamps
// anything above 11999 is the future

func createTestDbase() (db Database, err error) {
	clock.UseTestClock()
	defer clock.Goto(11999)

	cmd := exec.Command("rm", "-rf", DatabasePath)
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	options := &Options{
		BasePath:      DatabasePath,
		Resolution:    10,
		EpochDuration: 1000,
		PayloadSize:   4,
		SegmentLength: 100,
		MaxROEpochs:   2,
		MaxRWEpochs:   2,
	}

	db, err = New(options)
	if err != nil {
		return nil, err
	}

	// test cold data
	fields := []string{"a", "b", "c", "d"}
	value1 := []byte{3, 0, 3, 0}
	value2 := []byte{6, 0, 6, 0}

	clock.Goto(3999)
	if err := db.Put(3030, fields, value1); err != nil {
		return nil, err
	}

	clock.Goto(6999)
	if err := db.Put(6060, fields, value2); err != nil {
		return nil, err
	}

	return db, err
}

// deletes all files created for test db
// should be run at the end of each test
func cleanTestFiles() {
	cmd := exec.Command("rm", "-rf", DatabasePath)
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

// -------------------------------------------------------------------------- //

func TestNew(t *testing.T) {
	defer cleanTestFiles()

	db, err := createTestDbase()
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()
}

func TestPutGet(t *testing.T) {
	defer cleanTestFiles()

	db, err := createTestDbase()
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

func BenchmarkPut(b *testing.B) {
	defer cleanTestFiles()

	db, err := createTestDbase()
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
	defer cleanTestFiles()

	db, err := createTestDbase()
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
