package database

import (
	"io/ioutil"
	"os"
	"testing"
)

const (
	dir = "/tmp/test-database"
)

func TestLoadAll(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	// Test 1: valid database params file
	if err := os.MkdirAll(dir+"/test1", 0777); err != nil {
		t.Fatal(err)
	}

	data1 := []byte(`
  {
    "duration": 1,
    "retention": 2,
    "resolution": 3,
    "maxROEpochs": 4,
    "maxRWEpochs": 5
  }`)

	if err := ioutil.WriteFile(dir+"/test1/params.json", data1, 0777); err != nil {
		t.Fatal(err)
	}

	// Test 2: invalid json file
	if err := os.MkdirAll(dir+"/test2", 0777); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(dir+"/test2/params.json", []byte{}, 0777); err != nil {
		t.Fatal(err)
	}

	// Test 3: Invalid param values (duration, resolution)
	// TODO write test after writing some validation code

	// Test 4: Invalid param values (max ro/rw epoch count)
	// TODO write test after writing some validation code

	// Test LoadAll
	dbs := LoadAll(dir)

	if _, ok := dbs["test1"]; !ok {
		t.Fatal("missing db")
	}
	if _, ok := dbs["test2"]; ok {
		t.Fatal("invalid db")
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

// func TestTrack(t *testing.T) {
// 	if err := os.RemoveAll(dir); err != nil {
// 		t.Fatal(err)
// 	}
// 	if err := os.MkdirAll(dir, 0777); err != nil {
// 		t.Fatal(err)
// 	}
//
// 	p := &Params{
// 		Duration:    3600000000000,
// 		Retention:   36000000000000,
// 		Resolution:  60000000000,
// 		MaxROEpochs: 2,
// 		MaxRWEpochs: 2,
// 	}
//
// 	db, err := Open(dir, p)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
//
// 	fields := []string{"a", "b", "d"}
//
// 	if err := db.Track(uint64(p.Resolution*1), fields, 5, 1); err != nil {
// 		t.Fatal(err)
// 	}
// 	if err := db.Track(uint64(p.Resolution*2), fields, 5, 2); err != nil {
// 		t.Fatal(err)
// 	}
//
// 	if err := os.RemoveAll(dir); err != nil {
// 		t.Fatal(err)
// 	}
// }
