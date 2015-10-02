package block

import (
	"errors"
	"path"
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/kadirahq/go-tools/atomicplus"
	"github.com/kadirahq/go-tools/segmmap"
)

const (
	// block file prefix
	prefix = "block_"

	// Size of the segment file
	segsz = 1024 * 1024 * 20

	// A struct size depends on it's fields, field order and alignment (hardware).
	// The size of a point struct is 16 bytes (8B double + 8B uint64) when the
	// alignment is set to 8B or smaller. The init function checks this assertion.
	pointsz = 16
)

var (
	// ErrInvRange is returned when the range is invalid
	ErrInvRange = errors.New("invalid from/to range")
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
// ! TODO explain mapping and stuff.
type Block struct {
	records   [][]Point
	recsMtx   *sync.RWMutex
	segments  *segmmap.Map
	recLength int64
	recBytes  int64
	segRecs   int64
	emptyRec  []Point
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
// This increments the Total and Count by provided values
func (b *Block) Track(rid, pid int64, total float64, count uint64) (err error) {
	point, err := b.GetPoint(rid, pid)
	if err != nil {
		return err
	}

	// atomically increment total and count fields
	atomicplus.AddFloat64(&point.Total, total)
	atomic.AddUint64(&point.Count, count)

	return nil
}

// Fetch returns required subset of points from a record
func (b *Block) Fetch(rid, from, to int64) (res []Point, err error) {
	if from >= b.recLength || from < 0 ||
		to > b.recLength || to < 0 || to < from {
		return nil, ErrInvRange
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

// Sync synchronises data Points in memory to disk
// See https://godoc.org/github.com/kadirahq/go-tools/mmap#File.Sync
func (b *Block) Sync() error {
	return b.segments.Sync()
}

// Lock locks all block memory maps in physical memory.
// This operation may take some time on larger blocks.
func (b *Block) Lock() error {
	return b.segments.Lock()
}

// Close closes the block
func (b *Block) Close() error {
	return b.segments.Close()
}

// GetPoint checks if the record exists in the block and allocate new records if
// not and returns the requested
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

func (b *Block) readFileMap(id int64) {
	fileMap := b.segments.Maps[id]
	dataLength := int64(len(fileMap.Data))

	var rid int64
	for rid = 0; rid < dataLength; {
		rdata := fileMap.Data[rid : rid+b.recBytes]
		b.records = append(b.records, fromByteSlice(rdata))
		rid += b.recBytes
	}
}

func (b *Block) readRecords() {
	mapLen := int64(len(b.segments.Maps))

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
