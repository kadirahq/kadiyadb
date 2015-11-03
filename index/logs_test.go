package index

import (
	"os"
	"reflect"
	"strconv"
	"testing"
)

var (
	tmpdirlogs = "/tmp/test-logs/"
)

func setuplg(t testing.TB) func() {
	if err := os.RemoveAll(tmpdirlogs); err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(tmpdirlogs, 0777); err != nil {
		t.Fatal(err)
	}

	return func() {
		if err := os.RemoveAll(tmpdirlogs); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLogstore(t *testing.T) {
	defer setuplg(t)()

	l, err := NewLogs(tmpdirlogs)
	if err != nil {
		t.Fatal(err)
	}

	flds := []string{"r0", "b0"}
	node := WrapNode(&Node{RecordID: 0, Fields: flds})
	size := node.Node.Size()
	reqd := 1 + segszlogs/size

	for i := 0; i < reqd; i++ {
		istr := strconv.Itoa(i)
		flds := []string{"r" + istr, "b" + istr}
		node := WrapNode(&Node{RecordID: int64(i), Fields: flds})

		if err := l.Store(node); err != nil {
			t.Fatal(err)
		}
	}

	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}

	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = NewLogs(tmpdirlogs)
	if err != nil {
		t.Fatal(err)
	}

	tree, err := l.Load()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < reqd; i++ {
		istr := strconv.Itoa(i)
		flds := []string{"r" + istr, "b" + istr}

		res, err := tree.FindOne(flds)
		if err != nil {
			t.Fatal(err)
		} else if res == nil {
			t.Fatal("missing res")
		}

		if res.RecordID != int64(i) {
			t.Fatal("wrong record id")
		}

		if !reflect.DeepEqual(flds, res.Fields) || res.RecordID != int64(i) {
			t.Fatal("wrong value")
		}
	}

	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}
