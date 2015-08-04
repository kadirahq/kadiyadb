package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/elgs/gostrgen"
	"github.com/kadirahq/kadiyadb/index"
)

const (
	// FormatStr is used when printing test output
	FormatStr = "Chars: %2d | Fields: %3d, | Items: %6d | Size: %9d\n"
)

func main() {
	FieldChars := flag.Int("c", 50, "Number of characters in a field")
	flag.Parse()

	FieldCounts := []int{1, 5, 10}
	ItemCounts := []int{0, 1, 10, 100, 1000, 10000, 100000}

	for _, m := range ItemCounts {
		for _, n := range FieldCounts {
			size := getMemorySize(*FieldChars, n, m)
			fmt.Printf(FormatStr, *FieldChars, n, m, size)
		}
	}
}

func getMemorySize(fsize, fcount, icount int) int {
	writeIndex(fsize, fcount, icount)
	return readIndex()
}

func writeIndex(fsize, fcount, icount int) {
	os.RemoveAll("/tmp/i1")

	options := &index.Options{
		Path:  "/tmp/i1",
		ROnly: false,
	}

	idx, err := index.New(options)
	if err != nil {
		panic(err)
	}

	for i := 0; i < icount; i++ {
		fields := make([]string, fcount)
		for i := 0; i < fcount; i++ {
			randstr, err := gostrgen.RandGen(fsize, gostrgen.All, "", "")
			if err != nil {
				panic(err)
			}

			fields[i] = randstr
		}

		err = idx.Put(fields, 123)
		if err != nil {
			panic(err)
		}
	}

	err = idx.Close()
	if err != nil {
		panic(err)
	}
}

func readIndex() int {
	options := &index.Options{
		Path:  "/tmp/i1",
		ROnly: true,
	}

	m1 := &runtime.MemStats{}
	m2 := &runtime.MemStats{}

	runtime.GC()
	runtime.ReadMemStats(m1)

	idx, err := index.New(options)
	if err != nil {
		panic(err)
	}

	runtime.GC()
	runtime.ReadMemStats(m2)

	err = idx.Close()
	if err != nil {
		panic(err)
	}

	return int(m2.Alloc - m1.Alloc)
}
