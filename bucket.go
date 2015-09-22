package kadiradb

import (
	"path"
	"reflect"
	"unsafe"

	"github.com/kadirahq/go-tools/segmmap"
)

const (
	prefix = "bucket"

	// Size of the segment file
	segsz = 1024 * 1024 * 20

	// Point struct takes 16 bytes on a x64 machines.
	// So this works only on x64 s
	pointsz = 16
)

// Bucket is a collection of records.
type Bucket struct {
	Records [][]Point

	rsz  int64 // record size in points
	rbs  int64 // record size in bytes
	ssz  int64 // segment file size in points
	sfs  int64 // segment file size in bytes
	mmap *segmmap.Map
}

// Record is a collection of points.
type Record struct {
	Points []Point
}

// NewBucket creates a bucket.
func NewBucket(dir string, rsz int64) (b *Bucket, err error) {
	rbs := rsz * pointsz
	sfp := path.Join(dir, prefix)
	sfs := segsz - (segsz % rbs)
	ssz := sfs / rbs
	m, err := segmmap.NewMap(sfp, sfs)
	if err != nil {
		return nil, err
	}

	err = m.LoadAll()
	if err != nil {
		return nil, err
	}

	recs := [][]Point{}

	b = &Bucket{
		Records: recs,
		mmap:    m,
		rsz:     rsz,
		rbs:     rbs,
		ssz:     ssz,
		sfs:     sfs,
	}

	b.readRecords()

	return b, nil
}

func (b *Bucket) readRecords() {
	for i := 0; i < len(b.mmap.Maps); i++ {
		fileMap := b.mmap.Maps[i]

		var rid int64
		dataLength := int64(len(fileMap.Data))

		for rid = 0; rid < dataLength; {
			rdata := fileMap.Data[rid : rid+b.rbs]
			b.Records = append(b.Records, fromByteSlice(rdata))

			rid += b.rbs
		}
	}
}

}

func fromByteSlice(byteSlice []byte) []Point {
	head := (*reflect.SliceHeader)(unsafe.Pointer(&byteSlice))
	pointSliceHead := reflect.SliceHeader{
		Data: head.Data,
		Len:  head.Len / pointsz,
		Cap:  head.Cap / pointsz,
	}

	return *(*[]Point)(unsafe.Pointer(&pointSliceHead))
}
