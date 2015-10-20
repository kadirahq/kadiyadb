package block

import (
	"io"
	"path"
	"sync"

	"github.com/kadirahq/go-tools/fatomic"
	"github.com/kadirahq/go-tools/segments"
	"github.com/kadirahq/go-tools/segments/segmmap"
	"github.com/kadirahq/kadiyadb-protocol"
)

// RWBlock is a collection of records memory mapped to a set of segmented files.
// This makes it possible to perform atomic write operations on mapped values.
type RWBlock struct {
	records   [][]protocol.Point
	recsMtx   *sync.RWMutex
	segments  segments.Store
	recLength int64
	recBytes  int64
	segRecs   int64
	emptyRec  []protocol.Point
}

// NewRW function reads or creates a block on given directory.
// It will automatically load all existing block files.
func NewRW(dir string, rsz int64) (b *RWBlock, err error) {
	rbs := rsz * pointsz
	sfp := path.Join(dir, prefix)
	sfs := segsz - (segsz % rbs)
	ssz := sfs / rbs
	m, err := segmmap.New(sfp, sfs)
	if err != nil {
		return nil, err
	}

	b = &RWBlock{
		records:   [][]protocol.Point{},
		recsMtx:   new(sync.RWMutex),
		segments:  m,
		recLength: rsz,
		recBytes:  rbs,
		segRecs:   ssz,
		emptyRec:  make([]protocol.Point, rsz),
	}

	// This will use the segment.Read method until it reaches the EOF
	// Make sure no other operation uses segment.Read/Write methods.
	// If it becomes necessary, save the offset value in this struct.
	if err := b.readRecords(); err != nil {
		return nil, err
	}

	return b, nil
}

// Track adds a new set of point values to the Block
// This increments the Total and Count by given values
func (b *RWBlock) Track(rid, pid int64, total, count float64) (err error) {
	if pid < 0 || pid >= b.recLength {
		panic("point index is out of record bounds")
	}

	point, err := b.GetPoint(rid, pid)
	if err != nil {
		return err
	}

	// Atomically increment total and count fields.
	// As these memory locations are memory mapped,
	// this will be automatically saved to the disk.
	// This will have no effect on read-only blocks
	fatomic.AddFloat64(&point.Total, total)
	fatomic.AddFloat64(&point.Count, count)

	return nil
}

// Fetch returns required range of points from a single record
func (b *RWBlock) Fetch(rid, from, to int64) (res []protocol.Point, err error) {
	if from >= b.recLength || from < 0 ||
		to > b.recLength || to < 0 || to < from {
		panic("point index is out of record bounds")
	}

	record, err := b.GetRecord(rid)
	if err != nil {
		return nil, err
	}

	res = record[from:to]
	return res, nil
}

// Sync synchronises data Points in memory maps to disk storage
// This guarantees that the data is successfully written to disk
func (b *RWBlock) Sync() (err error) {
	return b.segments.Sync()
}

// Close releases resources
func (b *RWBlock) Close() (err error) {
	return b.segments.Close()
}

// GetRecord checks if the record exists in the block and returns it
// if it's available. Otherwise, it will return an empty point record.
func (b *RWBlock) GetRecord(rid int64) (rec []protocol.Point, err error) {
	b.recsMtx.RLock()
	// If `rid` is larger than or equal to the number of currently loaded records
	// it means that we don't have data for that yet. Return an empty data slice.
	if rid >= int64(len(b.records)) {
		b.recsMtx.RUnlock()
		rec = b.emptyRec
		return rec, nil
	}

	// slice result from record
	rec = b.records[rid]
	b.recsMtx.RUnlock()

	return rec, nil
}

// GetPoint checks if the record exists in the block and allocates
// new records if not and returns the point at requested position.
func (b *RWBlock) GetPoint(rid, pid int64) (point *protocol.Point, err error) {
	b.recsMtx.RLock()
	if rid < int64(len(b.records)) {
		point = &b.records[rid][pid]
		b.recsMtx.RUnlock()
		return point, nil
	}
	b.recsMtx.RUnlock()

	// Record is not present
	b.recsMtx.Lock()
	defer b.recsMtx.Unlock()

	if rid < int64(len(b.records)) {
		point = &b.records[rid][pid]
		return point, nil
	}

	off := rid * b.recBytes
	if err := b.segments.Ensure(off); err != nil {
		return nil, err
	}

	// This will continue from where it stopped when the Block struct was created
	// Make sure that no other operations use the segment.Read/Write methods.
	if err := b.readRecords(); err != nil {
		return nil, err
	}

	point = &b.records[rid][pid]

	return point, nil
}

// readRecords reads data files and converts it to a slices of records
// created records are then appended to b.records to use later
func (b *RWBlock) readRecords() (err error) {
	fsize := b.recBytes * b.segRecs

	for {
		data, err := b.segments.Slice(fsize)
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		var i int64
		for i = 0; i < b.segRecs; i++ {
			s := i * b.recBytes
			e := (i + 1) * b.recBytes
			p := data[s:e]
			b.records = append(b.records, decode(p))
		}
	}
}
