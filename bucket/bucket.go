package bucket

import (
	"path"
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/kadirahq/go-tools/atomicplus"
	"github.com/kadirahq/go-tools/segmmap"
)

const (
	prefix = "bucket"

	// Size of the segment file
	segsz = 1024 * 1024 * 20

	// A struct size depends on it's fields, field order and alignment (hardware).
	// The size of a point struct is 16 bytes (8B double + 8B uint64) when the
	// alignment is set to 8B or smaller. The init function checks this assertion.
	pointsz = 16
)

func init() {
	// Make sure that the point size is what we're expecting
	// it depends on hardware devices therefore can change.
	// Because of the way the point struct is made, it's highly
	// unlikely to change but it's better to verify on start.
	if unsafe.Sizeof(Point{}) != pointsz {
		panic("point size is different, possibly because of incompatible hardware")
	}
}

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

	b = &Bucket{
		Records: [][]Point{},
		mmap:    m,
		rsz:     rsz,
		rbs:     rbs,
		ssz:     ssz,
		sfs:     sfs,
	}

	var i int64
	mapLen := int64(len(b.mmap.Maps))
	for i = 0; i < mapLen; i++ {
		b.readFileMap(i)
	}

	return b, nil
}

// Add adds a new point to the Bucket
// This increments the Total and Count by the provided values
func (b *Bucket) Add(rid int64, pid int64, total float64, count uint64) error {
	// If rid is larger than currently loaded records, load a new segfile
	if rid >= int64(len(b.Records)) {
		segIndex := rid * b.rsz / b.ssz

		_, err := b.mmap.Load(segIndex)
		if err != nil {
			return err
		}

		b.readFileMap(segIndex)
	}

	atomicplus.AddFloat64(&(b.Records[rid][pid].Total), total)
	atomic.AddUint64(&(b.Records[rid][pid].Count), count)
	return nil
}

// Sync synchronises data Points in memory to disk
// See https://godoc.org/github.com/kadirahq/go-tools/mmap#File.Sync
func (b *Bucket) Sync() error {
	for _, memmap := range b.mmap.Maps {
		err := memmap.Sync()
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *Bucket) readFileMap(id int64) {
	fileMap := b.mmap.Maps[id]
	var rid int64
	dataLength := int64(len(fileMap.Data))

	for rid = 0; rid < dataLength; {
		rdata := fileMap.Data[rid : rid+b.rbs]
		b.Records = append(b.Records, fromByteSlice(rdata))
		rid += b.rbs
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
