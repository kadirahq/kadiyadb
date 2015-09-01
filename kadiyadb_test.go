package kadiyadb

import (
	"io/ioutil"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kadirahq/go-tools/logger"
	"github.com/kadirahq/go-tools/vtimer"
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
	vtimer.Use(vtimer.Test)
	vtimer.Set(11999)
	logger.Disable("time")
}

// A TEST CLOCK IS USED TO CONTROL THE TIME IN TESTS
// default future time range:      12000 --- .
// default read-write time range:  10000 --- 11999
// default read-only time range:    4000 ---  9999
// default expired time range:         0 ---  3999

func TestNew(t *testing.T) {
	if err := os.RemoveAll(DatabasePath); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	db, err := New(DefaultOptions)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := os.RemoveAll(DatabasePath); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}

func TestOpen(t *testing.T) {
	if err := os.RemoveAll(DatabasePath); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	db, err := New(DefaultOptions)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	// recovery mode set to false
	db, err = Open(DatabasePath, false)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	// recovery mode set to true
	db, err = Open(DatabasePath, true)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := os.RemoveAll(DatabasePath); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}

func TestEditMetadata(t *testing.T) {
	if err := os.RemoveAll(DatabasePath); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	db, err := New(DefaultOptions)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err = db.Edit(3, 3); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	info, err := db.Info()
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if info.MaxROEpochs != 3 ||
		info.MaxRWEpochs != 3 {
		t.Fatal("edit failed")
	}

	if err := db.Close(); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := os.RemoveAll(DatabasePath); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}

func TestPutGet(t *testing.T) {
	if err := os.RemoveAll(DatabasePath); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	db, err := New(DefaultOptions)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c", "d"}
	value1 := []byte{1, 2, 3, 4}
	value2 := []byte{5, 6, 7, 8}

	err = db.Put(10990, fields, value1)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	err = db.Put(11000, fields, value2)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	res, err := db.Get(10990, 11010, fields)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	// res only has one result
	for item, points := range res {
		if !reflect.DeepEqual(item.Fields, fields) ||
			!reflect.DeepEqual([][]byte{value1, value2}, points) {
			t.Fatal("incorrect results")
		}
	}

	if err := db.Close(); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := os.RemoveAll(DatabasePath); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}

func TestPutOldData(t *testing.T) {
	if err := os.RemoveAll(DatabasePath); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	db, err := New(DefaultOptions)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
	fields := []string{"a", "b", "c", "d"}
	value1 := []byte{1, 2, 3, 4}

	err = db.Put(9990, fields, value1)
	if err == nil {
		t.Fatal("should return epoch not found error")
	}

	if err := db.Close(); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := os.RemoveAll(DatabasePath); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}

func TestPutWithRec(t *testing.T) {
	if err := os.RemoveAll(DatabasePath); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	options := *DefaultOptions
	options.Recovery = true

	db, err := New(&options)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c", "d"}
	value1 := []byte{1, 2, 3, 4}

	err = db.Put(9990, fields, value1)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := os.RemoveAll(DatabasePath); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}

func TestExpireOldData(t *testing.T) {
	if err := os.RemoveAll(DatabasePath); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	db, err := New(DefaultOptions)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c", "d"}
	value := []byte{1, 2, 3, 4}

	vtimer.Set(5999)
	err = db.Put(4999, fields, value)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	err = db.Put(5999, fields, value)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	files, err := ioutil.ReadDir(DatabasePath)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if len(files) != 3 {
		t.Fatal("incorrect number of files")
	}

	vtimer.Set(11999)
	db, err = Open(DatabasePath, false)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	// run enforceRetention
	time.Sleep(time.Second)

	files, err = ioutil.ReadDir(DatabasePath)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if len(files) != 2 {
		t.Fatal("incorrect number of files")
	}

	out1, err := db.One(4990, 5000, fields)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if len(out1[0]) != 0 {
		t.Fatal("expired data should not exist")
	}

	out2, err := db.One(5990, 6000, fields)
	if err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if len(out2[0]) != 4 {
		t.Fatal("data should exist", out2)
	}

	if err := db.Close(); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}

	if err := os.RemoveAll(DatabasePath); err != nil {
		logger.Error(err)
		t.Fatal(err)
	}
}

func BenchmarkPut(b *testing.B) {
	if err := os.RemoveAll(DatabasePath); err != nil {
		b.Fatal(err)
	}

	db, err := New(DefaultOptions)
	if err != nil {
		b.Fatal(err)
	}

	value := []byte{1, 2, 3, 4}
	fields := []string{"a", "b", "c", "d"}

	var i int64

	b.ResetTimer()
	b.SetParallelism(1000)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&i, 1)
			ts := 11000 + (10*n)%1000
			if err := db.Put(ts, fields, value); err != nil {
				b.Fatal(err)
			}
		}
	})

	if err := db.Close(); err != nil {
		b.Fatal(err)
	}

	if err := os.RemoveAll(DatabasePath); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkGet(b *testing.B) {
	if err := os.RemoveAll(DatabasePath); err != nil {
		b.Fatal(err)
	}

	db, err := New(DefaultOptions)
	if err != nil {
		b.Fatal(err)
	}

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
	b.SetParallelism(1000)

	atomic.StoreInt64(&i, 0)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&i, 1)
			ts := 11000 + (10*n)%1000
			if _, err := db.Get(ts, ts+10, fields); err != nil {
				b.Fatal(err)
			}
		}
	})

	if err := db.Close(); err != nil {
		b.Fatal(err)
	}

	if err := os.RemoveAll(DatabasePath); err != nil {
		b.Fatal(err)
	}
}
