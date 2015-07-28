package main

import (
	"flag"
	"fmt"

	"github.com/elgs/gostrgen"
	"github.com/gogo/protobuf/proto"
	"github.com/kadirahq/kadiradb-core/index"
)

const (
	// FormatStr is used when printing test output
	FormatStr = "Chars: %2d | Fields: %3d | Size: %4d\n"
)

func main() {
	FieldChars := flag.Int("c", 50, "Number of characters in a field")
	flag.Parse()

	FieldCounts := []int{0, 1, 10, 100}

	for _, n := range FieldCounts {
		size := getDiskSize(*FieldChars, n)
		fmt.Printf(FormatStr, *FieldChars, n, size)
	}
}

// Size is calculated by creating an index item and marshalling it.
// The value is currently equal to 2 + FieldCounts * (FieldChars+2).
func getDiskSize(fsize, fcount int) int {
	item := makeUniqueItem(fsize, fcount)
	data, err := proto.Marshal(item)
	if err != nil {
		panic(err)
	}

	return len(data)
}

// Create an index item with random and unique fields
func makeUniqueItem(fsize, fcount int) *index.Item {
	fields := make([]string, fcount)
	for i := 0; i < fcount; i++ {
		randstr, err := gostrgen.RandGen(fsize, gostrgen.All, "", "")
		if err != nil {
			panic(err)
		}

		fields[i] = randstr
	}

	return &index.Item{Fields: fields}
}
