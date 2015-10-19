package kadiyadb

import (
	"io/ioutil"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/kadirahq/kadiyadb/block"
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
	data1 := []byte(`
  {
    "duration": 4,
    "resolution": 2,
    "retention": 8,
    "maxROEpochs": 10,
    "maxRWEpochs": 3
  }`)
	if err := os.MkdirAll(dir+"/test1", 0777); err != nil {
		t.Fatal(err)
	}
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
	data3 := []byte(`
  {
    "duration": 5,
    "resolution": 2,
    "retention": 8,
    "maxROEpochs": 10,
    "maxRWEpochs": 5
  }`)
	if err := os.MkdirAll(dir+"/test3", 0777); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(dir+"/test3/params.json", data3, 0777); err != nil {
		t.Fatal(err)
	}

	// Test 4: Invalid param values (max ro/rw epoch count)
	data4 := []byte(`
  {
    "duration": 4,
    "resolution": 2,
    "retention": 8,
    "maxROEpochs": 0,
    "maxRWEpochs": 5
  }`)
	if err := os.MkdirAll(dir+"/test4", 0777); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(dir+"/test4/params.json", data4, 0777); err != nil {
		t.Fatal(err)
	}

	// Test LoadAll
	dbs := LoadAll(dir)

	for _, name := range []string{"test1"} {
		if _, ok := dbs[name]; !ok {
			t.Fatal("missing db")
		}
	}

	for _, name := range []string{"test2", "test3", "test4"} {
		if _, ok := dbs[name]; ok {
			t.Fatal("invalid db")
		}
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestTrack(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	p := &Params{
		Duration:    3600000000000,
		Retention:   36000000000000,
		Resolution:  60000000000,
		MaxROEpochs: 2,
		MaxRWEpochs: 2,
	}

	db, err := Open(dir, p)
	if err != nil {
		t.Fatal(err)
	}

	fields := []string{"a", "b", "d"}

	if err := db.Track(uint64(p.Resolution*0), fields, 5, 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Track(uint64(p.Resolution*1), fields, 5, 2); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestFetchSimple(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	p := &Params{
		Duration:    3600000000000,
		Retention:   36000000000000,
		Resolution:  60000000000,
		MaxROEpochs: 2,
		MaxRWEpochs: 2,
	}

	db, err := Open(dir, p)
	if err != nil {
		t.Fatal(err)
	}

	fields := []string{"a", "b", "d"}

	if err := db.Track(uint64(p.Resolution*0), fields, 5, 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Track(uint64(p.Resolution*1), fields, 5, 2); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	db.Fetch(0, uint64(p.Resolution*2), fields, func(res []*Chunk, err error) {
		defer wg.Done()

		if len(res) != 1 {
			t.Fatal("wrong chunk count")
		}

		c := res[0]
		if c.From != 0 || c.To != uint64(p.Resolution*2) {
			t.Fatal("wrong chunk range")
		}

		if len(c.Series) != 1 {
			t.Fatal("wrong series count")
		}

		s := c.Series[0]
		points := []block.Point{{5, 1}, {5, 2}}

		if !reflect.DeepEqual(s.Fields, fields) {
			t.Fatal("wrong fields")
		}

		if !reflect.DeepEqual(s.Points, points) {
			t.Fatal("wrong fields")
		}
	})

	wg.Wait()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestFetchMultiSeries(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	p := &Params{
		Duration:    3600000000000,
		Retention:   36000000000000,
		Resolution:  60000000000,
		MaxROEpochs: 2,
		MaxRWEpochs: 2,
	}

	db, err := Open(dir, p)
	if err != nil {
		t.Fatal(err)
	}

	fields1 := []string{"a", "b", "c"}
	fields2 := []string{"a", "b", "d"}
	fieldsq := []string{"a", "b", "*"}

	if err := db.Track(uint64(p.Resolution*0), fields1, 5, 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Track(uint64(p.Resolution*1), fields2, 5, 2); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	db.Fetch(0, uint64(p.Resolution*2), fieldsq, func(res []*Chunk, err error) {
		defer wg.Done()

		if len(res) != 1 {
			t.Fatal("wrong chunk count")
		}

		c := res[0]
		if c.From != 0 || c.To != uint64(p.Resolution*2) {
			t.Fatal("wrong chunk range")
		}

		if len(c.Series) != 2 {
			t.Fatal("wrong series count")
		}

		s1 := c.Series[0]
		points1 := []block.Point{{5, 1}, {0, 0}}

		if !reflect.DeepEqual(s1.Fields, fields1) {
			t.Fatal("wrong fields")
		}

		if !reflect.DeepEqual(s1.Points, points1) {
			t.Fatal("wrong fields")
		}

		s2 := c.Series[1]
		points2 := []block.Point{{0, 0}, {5, 2}}

		if !reflect.DeepEqual(s2.Fields, fields2) {
			t.Fatal("wrong fields")
		}

		if !reflect.DeepEqual(s2.Points, points2) {
			t.Fatal("wrong fields")
		}
	})

	wg.Wait()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestFetchMultiChunk(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	p := &Params{
		Duration:    3600000000000,
		Retention:   36000000000000,
		Resolution:  60000000000,
		MaxROEpochs: 2,
		MaxRWEpochs: 2,
	}

	db, err := Open(dir, p)
	if err != nil {
		t.Fatal(err)
	}

	fields := []string{"a", "b", "d"}

	if err := db.Track(uint64(p.Duration-p.Resolution), fields, 5, 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Track(uint64(p.Duration), fields, 5, 2); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	db.Fetch(uint64(p.Duration-p.Resolution), uint64(p.Duration+p.Resolution), fields, func(res []*Chunk, err error) {
		defer wg.Done()

		if len(res) != 2 {
			t.Fatal("wrong chunk count")
		}

		c1 := res[0]
		if c1.From != uint64(p.Duration-p.Resolution) || c1.To != uint64(p.Duration) {
			t.Fatal("wrong chunk range")
		}

		if len(c1.Series) != 1 {
			t.Fatal("wrong series count")
		}

		s := c1.Series[0]
		points := []block.Point{{5, 1}}

		if !reflect.DeepEqual(s.Fields, fields) {
			t.Fatal("wrong fields")
		}

		if !reflect.DeepEqual(s.Points, points) {
			t.Fatal("wrong fields")
		}

		c2 := res[1]
		if c2.From != uint64(p.Duration) || c2.To != uint64(p.Duration+p.Resolution) {
			t.Fatal("wrong chunk range")
		}

		if len(c2.Series) != 1 {
			t.Fatal("wrong series count")
		}

		s = c2.Series[0]
		points = []block.Point{{5, 2}}

		if !reflect.DeepEqual(s.Fields, fields) {
			t.Fatal("wrong fields")
		}

		if !reflect.DeepEqual(s.Points, points) {
			t.Fatal("wrong fields")
		}
	})

	wg.Wait()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}
