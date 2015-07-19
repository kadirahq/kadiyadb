package block

import (
	"path"
	"strconv"
	"sync"

	"github.com/kadirahq/kadiradb-core/utils/logger"
	"github.com/kadirahq/kadiradb-core/utils/mmap"
)

const (
	// PreallocThresh is the minimum number record space to keep available
	// to have ready. Additional segments will be created automatically.
	PreallocThresh = 1000
)

type rwblock struct {
	*block                 // common block
	segments   []*mmap.Map // segment memory maps
	addMutex   *sync.Mutex // record allocation mutex
	allocMutex *sync.Mutex // segment allocation mutex
	allocating bool        // indicates whether a pre-alloc is in progress
}

func newRWBlock(b *block, options *Options) (blk *rwblock, err error) {
	segmentCount := b.metadata.SegmentCount

	blk = &rwblock{
		block:      b,
		segments:   make([]*mmap.Map, segmentCount),
		addMutex:   &sync.Mutex{},
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

	err = blk.preallocateIfNeeded()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	return blk, nil
}

func (b *rwblock) Add() (id int64, err error) {
	// force run allocation if we've already run out of space
	if b.availableRecordSpace() <= 0 {
		b.allocMutex.Lock()

		if b.availableRecordSpace() <= 0 {
			err = b.allocateSegment()
			if err != nil {
				b.allocMutex.Unlock()
				logger.Log(LoggerPrefix, err)
				return 0, err
			}

			b.metadata.SegmentCount++
		}

		b.allocMutex.Unlock()
	}

	// run pre-allocation in the background when we reach a threshold
	// check in order to avoid running unnecessary goroutines
	if !b.allocating && b.availableRecordSpace() < PreallocThresh {
		b.allocating = true
		go b.preallocateIfNeeded()
	}

	b.addMutex.Lock()
	nextID := b.metadata.RecordCount
	b.metadata.RecordCount++
	b.addMutex.Unlock()

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
	fpath := path.Join(b.opts.Path, SegmentFilePrefix+istr)
	size := b.metadata.SegmentLength * b.recordSize
	return mmap.New(&mmap.Options{Path: fpath, Size: size})
}

func (b *rwblock) availableRecordSpace() (n int64) {
	total := b.metadata.SegmentCount * b.metadata.SegmentLength
	return total - b.metadata.RecordCount
}

func (b *rwblock) preallocateIfNeeded() (err error) {
	if b.availableRecordSpace() < PreallocThresh {
		b.allocMutex.Lock()
		defer b.allocMutex.Unlock()

		if b.availableRecordSpace() < PreallocThresh {
			err = b.allocateSegment()
			if err != nil {
				logger.Log(LoggerPrefix, err)
				b.allocating = false
				return err
			}

			b.metadata.SegmentCount++
		}
	}

	b.allocating = false
	return nil
}

func (b *rwblock) allocateSegment() (err error) {
	mfile, err := b.loadSegment(b.metadata.SegmentCount)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	b.segments = append(b.segments, mfile)

	return nil
}
