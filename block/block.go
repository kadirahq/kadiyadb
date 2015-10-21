package block

import (
	"io"
	"reflect"
	"unsafe"

	"github.com/kadirahq/go-tools/fs"
	"github.com/kadirahq/kadiyadb-protocol"
)

const (
	// block file prefix
	// block files will be named "block_0, block_1, ..."
	prefix = "block_"

	// Size of the segment file
	// !IMPORTANT if this value changes, the database will not be able to use
	// older data. To avoid accidental changes, this value is hardcoded here.
	segsz = 1024 * 1024 * 200

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
	if unsafe.Sizeof(protocol.Point{}) != pointsz {
		panic("point size is different, possibly because of incompatible hardware")
	}
}

// Tracker provides a Track method to increment total and count values.
type Tracker interface {
	Track(rid, pid int64, total, count float64) (err error)
}

// Fetcher interface provides a Fetch method to read a slice of points
// from a record identified by a unique record id (records slice index).
type Fetcher interface {
	Fetch(rid, from, to int64) (res []protocol.Point, err error)
}

// Block is a collection of records (records are collections of Points).
// The block directly maps a slice of records which can be memory mapped
// and used as records using the unsafe package if necessary.
//
// Block File Format:
//
//      p0 p1 p2 p3 p4 p5 ..... pN
//   r0 .. .. .. .. .. .. .. .. ..
//   r1 .. .. .. .. .. .. .. .. ..
//   r2 .. .. .. .. .. .. .. .. ..
//   .. .. .. .. .. .. .. .. .. ..
//   rM .. .. .. .. .. .. .. .. ..
//
type Block interface {
	Tracker
	Fetcher
	fs.Syncer
	io.Closer
}

// decode maps given byte slice to a record made of points
// both the record and given data will share same memory
func decode(b []byte) []protocol.Point {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	ph := reflect.SliceHeader{
		Data: bh.Data,
		Len:  bh.Len / pointsz,
		Cap:  bh.Cap / pointsz,
	}

	return *(*[]protocol.Point)(unsafe.Pointer(&ph))
}
