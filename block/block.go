package block

import (
	"path"
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/kadirahq/go-tools/fatomic"
	"github.com/kadirahq/go-tools/segmap"
)

const (
	// block file prefix
	// block files will be named "block_0, block_1, ..."
	prefix = "block_"

	// Size of the segment file
	// !IMPORTANT if this value changes, the database will not be able to use
	// older data. To avoid accidental changes, this value is hardcoded here.
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

// Block is a collection of records (records are collections of Points).
// The block directly maps a slice of records to a set of mapped files.
// This is done by reusing the memory locations exposed by memory maps.
//
// Block File Format:
//
//      p0 p1 p2 p3 p4 p5 ..... pN
//   r0 .. .. .. .. .. .. .. .. ..
//   r1 .. .. .. .. .. .. .. .. ..
//   r2 .. .. .. .. .. .. .. .. ..
//      .. .. .. .. .. .. .. .. ..
//   rM .. .. .. .. .. .. .. .. ..
//
type Block struct {
	records   [][]Point
	recsMtx   *sync.RWMutex
	segments  *segmap.Store
	recLength int64
	recBytes  int64
	segRecs   int64
	emptyRec  []Point
}

// New function reads or creates a block on given directory.
// It will automatically load all existing block files.
func New(dir string, rsz int64) (b *Block, err error) {
	rbs := rsz * pointsz
	sfp := path.Join(dir, prefix)
	sfs := segsz - (segsz % rbs)
	ssz := sfs / rbs
	m, err := segmap.New(sfp, sfs)
	if err != nil {
		return nil, err
	}

	err = m.LoadAll()
	if err != nil {
		return nil, err
	}

	b = &Block{
		records:   [][]Point{},
		recsMtx:   new(sync.RWMutex),
		segments:  m,
		recLength: rsz,
		recBytes:  rbs,
		segRecs:   ssz,
		emptyRec:  make([]Point, rsz),
	}

	b.readRecords()

	return b, nil
}

// Track adds a new set of point values to the Block
// This increments the Total and Count by given values
func (b *Block) Track(rid, pid int64, total float64, count uint64) (err error) {
	if pid < 0 || pid >= b.recLength {
		panic("point index is out of record bounds")
	}

	point, err := b.GetPoint(rid, pid)
	if err != nil {
		return err
	}

	// atomically increment total and count fields
	fatomic.AddFloat64(&point.Total, total)
	atomic.AddUint64(&point.Count, count)

	return nil
}

// Fetch returns required range of points from a single record
func (b *Block) Fetch(rid, from, to int64) (res []Point, err error) {
	if from >= b.recLength || from < 0 ||
		to > b.recLength || to < 0 || to < from {
		panic("point index is out of record bounds")
	}

	b.recsMtx.RLock()
	// If `rid` is larger than or equal to the number of currently loaded records
	// it means that we don't have data for that yet. Return an empty data slice.
	if rid >= int64(len(b.records)) {
		res = b.emptyRec[from:to]
		return res, nil
	}

	// slice result from record
	res = b.records[rid][from:to]
	b.recsMtx.RUnlock()

	return res, nil
}

// Sync synchronises data Points in memory maps to disk storage
// This guarantees that the data is successfully written to disk
func (b *Block) Sync() error {
	return b.segments.Sync()
}

// Lock locks all block memory maps in physical memory.
// This operation may take some time on larger blocks.
func (b *Block) Lock() error {
	return b.segments.Lock()
}

// Close releases resources
func (b *Block) Close() error {
	return b.segments.Close()
}

// GetPoint checks if the record exists in the block and allocates
// new records if not and returns the point at requested position.
func (b *Block) GetPoint(rid, pid int64) (point *Point, err error) {
	b.recsMtx.RLock()
	if rid < int64(len(b.records)) {
		point = &b.records[rid][pid]
		b.recsMtx.RUnlock()
		return
	}
	b.recsMtx.RUnlock()

	// Record is not present
	b.recsMtx.Lock()
	if rid < int64(len(b.records)) {
		point = &b.records[rid][pid]
		b.recsMtx.Unlock()
		return
	}

	segIndex := rid / b.segRecs
	_, err = b.segments.Load(segIndex)
	if err != nil {
		b.recsMtx.Unlock()
		return
	}

	b.readFileMap(segIndex)
	point = &b.records[rid][pid]
	b.recsMtx.Unlock()

	return
}

// readFileMap reads a file and converts it to a slice of records
// created records are then appended to b.records to use later
func (b *Block) readFileMap(id int64) error {
	fileMap, err := b.segments.Load(id)
	if err != nil {
		return err
	}

	dataLength := int64(len(fileMap.Data))

	var rid int64
	for rid = 0; rid < dataLength; {
		rdata := fileMap.Data[rid : rid+b.recBytes]
		b.records = append(b.records, decode(rdata))
		rid += b.recBytes
	}

	return nil
}

// readRecords reads data files
func (b *Block) readRecords() {
	count := int64(b.segments.Length())

	var i int64
	for i = 0; i < count; i++ {
		b.readFileMap(i)
	}
}

// decode maps given byte slice to a record made of points
// both the record and given data will share same memory
func decode(byteSlice []byte) []Point {
	head := (*reflect.SliceHeader)(unsafe.Pointer(&byteSlice))
	pointSliceHead := reflect.SliceHeader{
		Data: head.Data,
		Len:  head.Len / pointsz,
		Cap:  head.Cap / pointsz,
	}

	return *(*[]Point)(unsafe.Pointer(&pointSliceHead))
}
