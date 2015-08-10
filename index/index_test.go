package index

import (
	"os"
	"reflect"
	"strconv"
	"testing"
)

func TestNewInitial(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewExisting(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	_, err := os.Create(fpath)
	if err != nil {
		t.Fatal(err)
	}

	idx, err := New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewCorrupt(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	file, err := os.Create(fpath)
	if err != nil {
		t.Fatal(err)
	}

	_, err = file.WriteString("invalid item")
	if err != nil {
		t.Fatal(err)
	}

	idx, err := New(&Options{Path: fpath})
	if idx != nil || err != ErrLoad {
		t.Fatal("should return an error")
	}
}

func TestPut(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c"}
	var value uint32 = 12345
	err = idx.Put(fields, value)
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestOne(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	fields1 := []string{"a", "b", "c"}
	var value1 uint32 = 12345
	err = idx.Put(fields1, value1)
	if err != nil {
		t.Fatal(err)
	}

	fields2 := []string{"a", "b"}
	var value2 uint32 = 12
	err = idx.Put(fields2, value2)
	if err != nil {
		t.Fatal(err)
	}

	item, err := idx.One(fields2)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(item.Fields, fields2) ||
		!reflect.DeepEqual(item.Value, value2) {
		t.Fatal("incorrect value")
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	idx, err = New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	item, err = idx.One(fields2)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(item.Fields, fields2) ||
		!reflect.DeepEqual(item.Value, value2) {
		t.Fatal("incorrect value")
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestGet(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c"}
	var value uint32 = 12345
	err = idx.Put(fields, value)
	if err != nil {
		t.Fatal(err)
	}

	items, err := idx.Get(fields)
	if err != nil {
		t.Fatal(err)
	}

	if len(items) != 1 {
		t.Fatal("incorrect number of results")
	}

	if res := items[0]; !reflect.DeepEqual(res.Fields, fields) ||
		!reflect.DeepEqual(res.Value, value) {
		t.Fatal("incorrect fields or value")
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	idx, err = New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	items, err = idx.Get(fields)
	if err != nil {
		t.Fatal(err)
	}

	if len(items) != 1 {
		t.Fatal("incorrect number of results")
	}

	if res := items[0]; !reflect.DeepEqual(res.Fields, fields) ||
		!reflect.DeepEqual(res.Value, value) {
		t.Fatal("incorrect fields or value")
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetWithWildcards(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c"}
	var value uint32 = 12345
	err = idx.Put(fields, value)
	if err != nil {
		t.Fatal(err)
	}

	// 2nd field and 3rd fields are wildcards
	// this will trigger a wildcard get
	query := []string{"a", "", ""}
	items, err := idx.Get(query)
	if err != nil {
		t.Fatal(err)
	}

	if len(items) != 1 {
		t.Fatal("incorrect number of results")
	}

	if res := items[0]; !reflect.DeepEqual(res.Fields, fields) ||
		!reflect.DeepEqual(res.Value, value) {
		t.Fatal("incorrect fields or value")
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	idx, err = New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	items, err = idx.Get(query)
	if err != nil {
		t.Fatal(err)
	}

	if len(items) != 1 {
		t.Fatal("incorrect number of results")
	}

	if res := items[0]; !reflect.DeepEqual(res.Fields, fields) ||
		!reflect.DeepEqual(res.Value, value) {
		t.Fatal("incorrect fields or value")
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetWithFilter(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	fields := []string{"a", "b", "c"}
	var value uint32 = 12345
	err = idx.Put(fields, value)
	if err != nil {
		t.Fatal(err)
	}

	// 2nd field is a wildcard and third field exists
	// this will trigger a wildcard get and a filter
	query := []string{"a", "", "c"}
	items, err := idx.Get(query)
	if err != nil {
		t.Fatal(err)
	}

	if len(items) != 1 {
		t.Fatal("incorrect number of results")
	}

	if res := items[0]; !reflect.DeepEqual(res.Fields, fields) ||
		!reflect.DeepEqual(res.Value, value) {
		t.Fatal("incorrect fields or value")
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	idx, err = New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	items, err = idx.Get(query)
	if err != nil {
		t.Fatal(err)
	}

	if len(items) != 1 {
		t.Fatal("incorrect number of results")
	}

	if res := items[0]; !reflect.DeepEqual(res.Fields, fields) ||
		!reflect.DeepEqual(res.Value, value) {
		t.Fatal("incorrect fields or value")
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSaveLoadSnapshot(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)
	defer os.Remove("/tmp/i1.snap-root")
	defer os.Remove("/tmp/i1.snap-data")

	idx, err := New(&Options{Path: fpath})
	if err != nil {
		t.Fatal(err)
	}

	var value uint32 = 12345
	fields1 := []string{"a", "b", "c"}
	fields2 := []string{"a", "d", "e"}
	fields3 := []string{"f", "g", "h"}

	err = idx.Put(fields1, value)
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Put(fields2, value)
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Put(fields3, value)
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	// trigger a snapshot save
	idx, err = New(&Options{Path: fpath, ROnly: true})
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	// trigger a snapshot load
	idx, err = New(&Options{Path: fpath, ROnly: true})
	if err != nil {
		t.Fatal(err)
	}

	idxx := idx.(*index)
	if idxx.rootNode.children["a"].children != nil ||
		idxx.rootNode.children["f"].children != nil {
		t.Fatal("only level1 should be loaded")
	}

	_, err = idx.Get([]string{"a"})
	if err != nil {
		t.Fatal(err)
	}

	if idxx.rootNode.children["a"].children == nil ||
		idxx.rootNode.children["f"].children != nil {
		t.Fatal("only branch 'a' should be loaded")
	}

	_, err = idx.Get([]string{"f"})
	if err != nil {
		t.Fatal(err)
	}

	if idxx.rootNode.children["a"].children == nil ||
		idxx.rootNode.children["f"].children == nil {
		t.Fatal("branches 'a' and 'f' should be loaded")
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func BenchPutDepth(b *testing.B, depth int) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := New(&Options{Path: fpath})
	if err != nil {
		b.Fatal(err)
	}

	items := make([]Item, b.N)
	for i := 0; i < b.N; i++ {
		fields := make([]string, depth)
		for j := 0; j < depth; j++ {
			fields[j] = "a"
		}

		item := Item{
			Fields: fields,
			Value:  12345,
		}

		// randomize fields
		item.Fields[i%depth] += strconv.Itoa(i)
		items[i] = item
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := items[i]
		err = idx.Put(item.Fields, item.Value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPut3(b *testing.B)  { BenchPutDepth(b, 3) }
func BenchmarkPut6(b *testing.B)  { BenchPutDepth(b, 6) }
func BenchmarkPut30(b *testing.B) { BenchPutDepth(b, 30) }

func BenchGetDepth(b *testing.B, depth int) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := New(&Options{Path: fpath})
	if err != nil {
		b.Fatal(err)
	}

	items := make([]Item, b.N)
	for i := 0; i < b.N; i++ {
		fields := make([]string, depth)
		for j := 0; j < depth; j++ {
			fields[j] = "a"
		}

		item := Item{
			Fields: fields,
			Value:  12345,
		}

		// randomize fields
		item.Fields[i%depth] += strconv.Itoa(i)
		items[i] = item
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := items[i]
		_, err = idx.Get(item.Fields)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet3(b *testing.B)  { BenchGetDepth(b, 3) }
func BenchmarkGet6(b *testing.B)  { BenchGetDepth(b, 6) }
func BenchmarkGet30(b *testing.B) { BenchGetDepth(b, 30) }

func BenchGetFilteredDepth(b *testing.B, depth int) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := New(&Options{Path: fpath})
	if err != nil {
		b.Fatal(err)
	}

	items := make([]Item, b.N)
	for i := 0; i < b.N; i++ {
		fields := make([]string, depth)
		for j := 0; j < depth; j++ {
			fields[j] = "a"
		}

		item := Item{
			Fields: fields,
			Value:  12345,
		}

		// randomize fields
		item.Fields[i%depth] += strconv.Itoa(i)
		items[i] = item
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := items[i]
		item.Fields[depth/2] = ""

		_, err = idx.Get(item.Fields)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetFiltered3(b *testing.B)  { BenchGetFilteredDepth(b, 3) }
func BenchmarkGetFiltered6(b *testing.B)  { BenchGetFilteredDepth(b, 6) }
func BenchmarkGetFiltered30(b *testing.B) { BenchGetFilteredDepth(b, 30) }
