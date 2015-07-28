package main

import (
	"flag"
	"fmt"
)

const (
	// SegmentSize is the size of a segment file
	SegmentSize = 120 * 1024 * 1024

	// SegmentPrealloc is the threshold value to pre allocate new segment
	// This is the minimum number of records space to keep available.
	SegmentPrealloc = 1000

	// IndexFieldOverhead is the number of bytes overhead per field
	IndexFieldOverhead = 2

	// IndexItemOverhead is the number of bytes overhead per item
	IndexItemOverhead = 2

	// IndexHeaderSize is the number of bytes used to store index size
	IndexHeaderSize = 4

	// IndexItemOnTreeSz is the number of bytes required to store an index item
	// in memory. This number is an estimate and a worst case scenario. This value
	// is obtained when there are 5 fields and all of them are unique.
	IndexItemOnTreeSz = 1800

	// BytesPerMB is the number of bytes per megabyte
	BytesPerMB = 1024 * 1024
)

func main() {
	PayloadSize := flag.Int64("p", 12, "Payload size in bytes")
	RecordLength := flag.Int64("d", 180, "Number of points in a record")
	RecordCount := flag.Int64("r", 1000000, "Number of records in the database")
	FieldCount := flag.Int64("f", 5, "Number of fields in index item")
	FieldChars := flag.Int64("c", 50, "Number of characters in a field")
	MaxRWEpochs := flag.Int64("rw", 2, "Number of read-write epochs")
	MaxROEpochs := flag.Int64("ro", 10, "Number of read-write epochs")
	flag.Parse()

	RecordSize := *RecordLength * *PayloadSize
	SegmentLength := SegmentSize / RecordSize

	SegmentCount := (*RecordCount + SegmentPrealloc) / SegmentLength
	if (*RecordCount+SegmentPrealloc)%SegmentLength != 0 {
		SegmentCount++
	}

	TotalBlockSize := SegmentCount * SegmentSize

	IndexFieldSize := *FieldChars + IndexFieldOverhead
	IndexItemSize := *FieldCount*IndexFieldSize + IndexItemOverhead
	IndexItemOnDisk := IndexHeaderSize + IndexItemSize

	TotalIndexSize := *RecordCount * IndexItemOnDisk
	TotalIndexTreeSize := *RecordCount * IndexItemOnTreeSz

	RWEpochMemory := TotalBlockSize + TotalIndexSize + TotalIndexTreeSize
	ROEpochMemory := TotalIndexTreeSize

	fmt.Println("\n** Parameters **")
	fmt.Printf("PayloadSize:     %8d bytes \n", PayloadSize)
	fmt.Printf("RecordLength:    %8d points \n", RecordLength)
	fmt.Printf("RecordCount:     %8d records \n", RecordCount)
	fmt.Printf("FieldCount:      %8d fields \n", FieldCount)
	fmt.Printf("FieldChars:      %8d chars \n", FieldChars)
	fmt.Printf("MaxRWEpochs:     %8d epochs \n", MaxRWEpochs)
	fmt.Printf("MaxROEpochs:     %8d epochs \n", MaxROEpochs)

	fmt.Println("\n** Disk Space Requirements **")
	fmt.Printf("Total Segment Size: %12d MB \n", TotalBlockSize/BytesPerMB)
	fmt.Printf("Total Index Size:   %12d MB \n", TotalIndexSize/BytesPerMB)

	fmt.Println("\n** Memory Requirements **")
	fmt.Printf("Memory Per RW Epoch: %12d MB \n", RWEpochMemory/BytesPerMB)
	fmt.Printf("Memory Per RO Epoch: %12d MB \n", ROEpochMemory/BytesPerMB)
	fmt.Printf("Total Memory for RW: %12d MB \n", RWEpochMemory**MaxRWEpochs/BytesPerMB)
	fmt.Printf("Total Memory for RO: %12d MB \n", ROEpochMemory**MaxROEpochs/BytesPerMB)
}
