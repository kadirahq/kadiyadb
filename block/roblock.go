package block

import (
	"errors"
	"os"
	"path"
	"strconv"
	"sync/atomic"
)

// Segment file parameters. This is used with read-only blocks where we
// read from and write to files directly instead of using a memory map
const (
	SegOpenRO = os.O_RDONLY
	SegPermRO = 0644
)

var (
	// ErrNoSeg is returned when requesting a segment which doesn't exist
	ErrNoSeg = errors.New("segment file doesn't exist for record")

	// ErrClose is returned when closing open segment files fails
	ErrClose = errors.New("segment file could not be closed")
)

type roblock struct {
	*block            // common block
	files  []*os.File // segment files
}

// NewRO creates a read-only block
func NewRO(cb *block, options *Options) (b Block, err error) {
	segments := cb.metadata.Segments
	files := make([]*os.File, segments)

	var i uint32
	for i = 0; i < segments; i++ {
		istr := strconv.Itoa(int(i))
		fpath := path.Join(options.Path, SegPrefix+istr)
		file, err := os.OpenFile(fpath, SegOpenRO, SegPermRO)
		if err != nil {
			Logger.Trace(err)
			return nil, err
		}

		files[i] = file
	}

	b = &roblock{
		block: cb,
		files: files,
	}

	return b, nil
}

func (b *roblock) Add() (id uint32, err error) {
	Logger.Trace(ErrROnly)
	return 0, ErrROnly
}

func (b *roblock) Put(id, pos uint32, pld []byte) (err error) {
	return ErrROnly
}

func (b *roblock) Get(id, start, end uint32) (res [][]byte, err error) {
	if end > b.options.RSize || start < 0 {
		Logger.Trace(ErrBound)
		return nil, ErrBound
	}

	payloadSize := b.options.PSize
	segmentSize := b.metadata.Records
	segmentNumber := id / segmentSize

	if segmentNumber < 0 || segmentNumber >= b.metadata.Segments {
		Logger.Trace(ErrNoSeg)
		return nil, ErrNoSeg
	}

	file := b.files[segmentNumber]
	seriesLength := end - start
	seriesSize := seriesLength * payloadSize
	seriesData := make([]byte, seriesSize)

	// record position inside the segment
	recordPosition := id % segmentSize
	recordSize := payloadSize * b.options.RSize
	startOffset := int64(recordPosition*recordSize + start*payloadSize)

	n, err := file.ReadAt(seriesData, startOffset)
	if err != nil {
		Logger.Trace(err)
		return nil, err
	} else if uint32(n) != seriesSize {
		Logger.Trace(ErrRead)
		return nil, ErrRead
	}

	res = make([][]byte, seriesLength)

	var i uint32
	for i = 0; i < seriesLength; i++ {
		res[i] = seriesData[i*payloadSize : (i+1)*payloadSize]
	}

	atomic.AddInt64(&b.metrics.GetOps, 1)
	return res, nil
}

func (b *roblock) Close() (err error) {
	err = b.block.Close()
	if err != nil {
		Logger.Trace(err)
		return err
	}

	var lastErr error
	for _, file := range b.files {
		err = file.Close()
		if err != nil {
			lastErr = err
		}
	}

	Logger.Trace(err)
	if lastErr != nil {
		return ErrClose
	}

	return nil
}
