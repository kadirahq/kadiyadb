package kadiradb

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
	var i int64
	mapLen := int64(len(b.mmap.Maps))
	for i = 0; i < mapLen; i++ {
		b.readFileMap(i)
	}
}

// Add adds a new point to the Bucket
// This increments the Total and Count by the provided values
func (b *Bucket) Add(recordID int64, pointID int64,
	total float64, count uint32) error {
	if recordID >= int64(len(b.Records)) {
		// If recordID is larger than currently loaded records we need to load a
		// new segfile
		segIndex := recordID * b.rsz / b.ssz

		_, err := b.mmap.Load(segIndex)
		if err != nil {
			return err
		}

		b.readFileMap(segIndex)
	}

	atomicplus.AddFloat64(&(b.Records[recordID][pointID].Total), total)
	atomic.AddUint32(&(b.Records[recordID][pointID].Count), count)
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
