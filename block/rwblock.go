package block

import (
	"path"
	"strconv"
	"sync"

	"github.com/meteorhacks/kadiradb-core/utils/logger"
	"github.com/meteorhacks/kadiradb-core/utils/mmap"
)

type rwblock struct {
	*block
	segments   []*mmap.Map
	allocMutex *sync.Mutex
}

func newRWBlock(b *block, options *Options) (blk *rwblock, err error) {
	segmentCount := b.metadata.SegmentCount

	blk = &rwblock{
		block:      b,
		segments:   make([]*mmap.Map, segmentCount),
		allocMutex: &sync.Mutex{},
	}

	var i int64
	for i = 0; i < segmentCount; i++ {
		blk.segments[i], err = blk.loadSegment(i)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return nil, err
		}
	}

	return blk, nil
}

func (b *rwblock) Add() (id int64, err error) {
	b.allocMutex.Lock()
	defer b.allocMutex.Unlock()

	nextID := b.metadata.RecordCount
	capacity := b.metadata.SegmentLength * b.metadata.SegmentCount

	if nextID >= capacity {
		mfile, err := b.loadSegment(b.metadata.SegmentCount)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return 0, err
		}

		b.segments = append(b.segments, mfile)
		b.metadata.SegmentCount++
	}

	b.metadata.RecordCount++
	err = b.saveMetadata()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return 0, err
	}

	return nextID, nil
}

func (b *rwblock) Put(id, pos int64, pld []byte) (err error) {
	if pos > b.opts.PayloadCount || pos < 0 {
		logger.Log(LoggerPrefix, ErrOutOfBounds)
		return ErrOutOfBounds
	}

	if int64(len(pld)) != b.opts.PayloadSize {
		logger.Log(LoggerPrefix, ErrWrongSize)
		return ErrWrongSize
	}

	segmentSize := b.metadata.SegmentLength
	segmentNumber := id / segmentSize

	if segmentNumber >= b.metadata.SegmentCount {
		logger.Log(LoggerPrefix, ErrNoSegment)
		return ErrNoSegment
	}

	// record position inside the segment
	recordPosition := id % segmentSize
	offset := recordPosition*b.recordSize + pos*b.opts.PayloadSize
	mfile := b.segments[segmentNumber]

	var i int64
	for i = 0; i < b.opts.PayloadSize; i++ {
		mfile.Data[offset+i] = pld[i]
	}

	return nil
}

func (b *rwblock) Get(id, start, end int64) (res [][]byte, err error) {
	if end > b.opts.PayloadCount || start < 0 {
		logger.Log(LoggerPrefix, ErrOutOfBounds)
		return nil, ErrOutOfBounds
	}

	segmentSize := b.metadata.SegmentLength
	segmentNumber := id / segmentSize

	if segmentNumber >= b.metadata.SegmentCount {
		logger.Log(LoggerPrefix, ErrNoSegment)
		return nil, ErrNoSegment
	}

	// record position inside the segment
	recordPosition := id % segmentSize
	startOffset := recordPosition*b.recordSize + start*b.opts.PayloadSize
	mfile := b.segments[segmentNumber]

	seriesLength := end - start
	seriesSize := seriesLength * b.opts.PayloadSize
	seriesData := mfile.Data[startOffset : startOffset+seriesSize]
	res = make([][]byte, seriesLength)

	var i int64
	for i = 0; i < seriesLength; i++ {
		res[i] = seriesData[i*b.opts.PayloadSize : (i+1)*b.opts.PayloadSize]
	}

	return res, nil
}

func (b *rwblock) Close() (err error) {
	err = b.block.Close()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	for _, seg := range b.segments {
		err = seg.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}
	}

	return nil
}

func (b *rwblock) loadSegment(id int64) (mfile *mmap.Map, err error) {
	istr := strconv.Itoa(int(id))
	fpath := path.Join(b.opts.Path, "segment_"+istr)
	size := b.metadata.SegmentLength * b.recordSize
	return mmap.New(&mmap.Options{Path: fpath, Size: size})
}
