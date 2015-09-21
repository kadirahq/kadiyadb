package kadiradb

import (
	"path"

	"github.com/kadirahq/go-fsutil/segmmap"
)

const (
	prefix  = "bucket"
	segsz   = 1024 * 1024 * 100
	pointsz = 16
)

// Bucket is a collection of records.
type Bucket struct {
	Records []Record

	rsz int   // record size (points)
	rbs int64 // record byte size
}

// Record is a collection of points.
type Record struct {
	Points []Point
}

// NewBucket creates a bucket.
func NewBucket(dir string, rsz int) (b *Bucket, err error) {
	rbs := rsz * pointsz
	sfp := path.Join(dir, prefix)
	sfs := segsz - (segsz % rbs)
	m, err := segmmap.NewMap(sfp, sfs, true)
	if err != nil {
		return nil, err
	}

	recs := []Record{}
	// TODO map recs

	b = &Bucket{
		Records: recs,
		rsz:     rsz,
		rbs:     rbs,
	}

	return b, nil
}
