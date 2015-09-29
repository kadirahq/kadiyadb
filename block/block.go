package block

import (
	"errors"
	"path"
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/kadirahq/go-tools/atomicplus"
	"github.com/kadirahq/go-tools/segmmap"
)

const (
	prefix = "block_"

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

// Block is a collection of records.
type Block struct {
	recs [][]Point
	mmap *segmmap.Map

	rsz int64 // record size in points
	rbs int64 // record size in bytes
	ssz int64 // segment file size in points
	sfs int64 // segment file size in bytes
}

// Record is a collection of points.
type Record struct {
	Points []Point
}

// New creates a block.
func New(dir string, rsz int64) (b *Block, err error) {
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

	b = &Block{
		recs: [][]Point{},
		mmap: m,
		rsz:  rsz,
		rbs:  rbs,
		ssz:  ssz,
		sfs:  sfs,
	}

	b.readRecords()

	return b, nil
}

// Track adds a new point to the Block
// This increments the Total and Count by the provided values
func (b *Block) Track(rid, pid int64, total float64, count uint64) (err error) {
	// If `rid` is larger than currently loaded records, load a new segfile
	if rid >= int64(len(b.recs)) {
		segIndex := rid * b.rsz / b.ssz

		_, err := b.mmap.Load(segIndex)
		if err != nil {
			return err
		}

		b.readFileMap(segIndex)
	}

	// atomically increment both fields. No need to use a mutex.
	point := &b.recs[rid][pid]
	atomicplus.AddFloat64(&point.Total, total)
	atomic.AddUint64(&point.Count, count)

	return nil
}

// Fetch returns required subset of points from a record
func (b *Block) Fetch(rid, from, to int64) (res []Point, err error) {
	if from >= b.rsz || from < 0 ||
		to >= b.rsz || to < 0 || to < from {
		// TODO export and reuse error
		return nil, errors.New("invalid range")
	}

	// If `rid` is larger than currently loaded records
	if rid >= int64(len(b.recs)) {
		// TODO export and reuse error
		return nil, errors.New("record not found")
	}

	// slice result from record
	res = b.recs[rid][from:to]

	return res, nil
}

// Sync synchronises data Points in memory to disk
// See https://godoc.org/github.com/kadirahq/go-tools/mmap#File.Sync
func (b *Block) Sync() error {
	return b.mmap.Sync()
}

// Lock locks all block memory maps in physical memory.
// This operation may take some time on larger blocks.
func (b *Block) Lock() error {
	return b.mmap.Lock()
}

// Close closes the block
func (b *Block) Close() error {
	return b.mmap.Close()
}

func (b *Block) readFileMap(id int64) {
	fileMap := b.mmap.Maps[id]
	dataLength := int64(len(fileMap.Data))

	var rid int64
	for rid = 0; rid < dataLength; {
		rdata := fileMap.Data[rid : rid+b.rbs]
		b.recs = append(b.recs, fromByteSlice(rdata))
		rid += b.rbs
	}
}

func (b *Block) readRecords() {
	mapLen := int64(len(b.mmap.Maps))

	var i int64
	for i = 0; i < mapLen; i++ {
		b.readFileMap(i)
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
