package block

import (
	"path"

	"github.com/kadirahq/go-tools/segments"
	"github.com/kadirahq/go-tools/segments/segfile"
)

// ROBlock is a collection of records read from a set of segmented files.
// This block type can only perform read operations and makes garbage.
type ROBlock struct {
	segments  segments.Store
	recLength int64
	recBytes  int64
	emptyRec  []Point
}

// NewRO function reads a block on given directory.
// It will read data from segment files when required.
func NewRO(dir string, rsz int64) (b *ROBlock, err error) {
	rbs := rsz * pointsz
	sfp := path.Join(dir, prefix)
	sfs := segsz - (segsz % rbs)
	m, err := segfile.New(sfp, sfs)
	if err != nil {
		return nil, err
	}

	b = &ROBlock{
		segments:  m,
		recLength: rsz,
		recBytes:  rbs,
		emptyRec:  make([]Point, rsz),
	}

	return b, nil
}

// Track method is not supported in read-only blocks so should not be called
func (b *ROBlock) Track(rid, pid int64, total float64, count uint64) (err error) {
	panic("write on read-only block")
}

// Fetch returns required range of points from a single record
func (b *ROBlock) Fetch(rid, from, to int64) (res []Point, err error) {
	if from >= b.recLength || from < 0 ||
		to > b.recLength || to < 0 || to < from {
		panic("point index is out of record bounds")
	}

	num := (to - from)
	res = make([]Point, num)

	off := rid*b.recBytes + from*pointsz
	p, err := b.segments.SliceAt(num*pointsz, off)
	if err != nil {
		return nil, err
	}

	// NOTE: p will be collected as garbage as decode is done by unsafe
	// a copy of the point slice should be made to make the result valid.
	// This sucks, but there's no good way to keep a reference to p data.
	copy(res, decode(p))

	return res, nil
}

// Sync is unnecessary for reaf-only blocks so should not be called
func (b *ROBlock) Sync() (err error) {
	panic("sync on read-only block")
}

// Close releases resources
func (b *ROBlock) Close() (err error) {
	return b.segments.Close()
}
