package index

import (
	"os"
	"reflect"
	"strconv"
	"testing"
)

var (
	tmpdirsnap = "/tmp/test-snap/"
)

func setupro(t testing.TB) func() {
	if err := os.RemoveAll(tmpdirsnap); err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(tmpdirsnap, 0777); err != nil {
		t.Fatal(err)
	}

	return func() {
		if err := os.RemoveAll(tmpdirsnap); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSnapshot(t *testing.T) {
	defer setupro(t)()

	tree := WrapNode(nil)
	for i := 0; i < 3; i++ {
		istr := strconv.Itoa(i)
		flds := []string{"r" + istr, "b" + istr}
		tree.Ensure(flds).Node.RecordID = int64(i)
	}

	s, err := writeSnapshot(tmpdirsnap, tree)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		istr := strconv.Itoa(i)
		flds := []string{"r" + istr, "b" + istr}

		res, err := s.RootNode.FindOne(flds)
		if err != nil {
			t.Fatal(err)
		} else if res == nil {
			t.Fatal("missing res")
		}

		if !reflect.DeepEqual(flds, res.Fields) || res.RecordID != int64(i) {
			t.Fatal("wrong value")
		}
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if s, err = LoadSnap(tmpdirsnap); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 3; i++ {
			istr := strconv.Itoa(i)
			name := "r" + istr

			res, ok := s.RootNode.Children[name]
			if !ok {
				t.Fatal("should have entry")
			} else if res != nil {
				t.Fatal("should be nil")
			}
		}

		for i := 0; i < 3; i++ {
			istr := strconv.Itoa(i)

			br, err := s.LoadBranch("r" + istr)
			if err != nil {
				t.Fatal(err)
			}

			bb, ok := br.Children["b"+istr]
			if !ok || bb == nil {
				t.Fatal("invalid child node")
			}

			ex := tree.Children["r"+istr].Children["b"+istr]
			if bb.Node.RecordID != ex.Node.RecordID {
				t.Fatal("wrong node value")
			}
		}

		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
