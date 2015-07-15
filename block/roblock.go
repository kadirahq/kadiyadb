package block

import (
	"errors"
	"os"
	"path"
	"strconv"

	"github.com/meteorhacks/kadiradb-core/utils/logger"
)

var (
	// ErrNoSegment is returned when requesting a segment which doesn't exist
	ErrNoSegment = errors.New("segment file doesn't exist for record")
)

type roblock struct {
	*block              // common block
	segments []*os.File // segment files
}

func newROBlock(b *block, options *Options) (blk *roblock, err error) {
	segmentCount := b.metadata.SegmentCount
	segments := make([]*os.File, segmentCount)

	var i int64
	for i = 0; i < segmentCount; i++ {
		istr := strconv.Itoa(int(i))
		fpath := path.Join(options.Path, SegmentFilePrefix+istr)
		file, err := os.OpenFile(fpath, SegmentOpenMode, SegmentPermissions)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return nil, err
		}

		segments[i] = file
	}

	blk = &roblock{
		block:    b,
		segments: segments,
	}

	return blk, nil
}

func (b *roblock) Add() (id int64, err error) {
	return 0, ErrReadOnly
}

func (b *roblock) Put(id, pos int64, pld []byte) (err error) {
	return ErrReadOnly
}

func (b *roblock) Get(id, start, end int64) (res [][]byte, err error) {
	if end > b.opts.PayloadCount || start < 0 {
		logger.Log(LoggerPrefix, ErrOutOfBounds)
		return nil, ErrOutOfBounds
	}

	segmentSize := b.metadata.SegmentLength
	segmentNumber := id / segmentSize

	if segmentNumber < 0 || segmentNumber >= b.metadata.SegmentCount {
		logger.Log(LoggerPrefix, ErrNoSegment)
		return nil, ErrNoSegment
	}

	file := b.segments[segmentNumber]
	seriesLength := end - start
	seriesSize := seriesLength * b.opts.PayloadSize
	seriesData := make([]byte, seriesSize)

	// record position inside the segment
	recordPosition := id % segmentSize
	startOffset := recordPosition*b.recordSize + start*b.opts.PayloadSize

	n, err := file.ReadAt(seriesData, startOffset)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	} else if int64(n) != seriesSize {
		logger.Log(LoggerPrefix, ErrRead)
		return nil, ErrRead
	}

	res = make([][]byte, seriesLength)

	var i int64
	for i = 0; i < seriesLength; i++ {
		res[i] = seriesData[i*b.opts.PayloadSize : (i+1)*b.opts.PayloadSize]
	}

	return res, nil
}

func (b *roblock) Close() (err error) {
	err = b.block.Close()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	for _, file := range b.segments {
		err = file.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}
	}

	return nil
}
