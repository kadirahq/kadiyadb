package block

import (
	"path"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/kadirahq/kadiyadb/utils/mmap"
)

type rwblock struct {
	*block                 // common block
	segments   []*mmap.Map // segment memory maps
	addMutex   *sync.Mutex // record allocation mutex
	allocMutex *sync.Mutex // segment allocation mutex
	allocating bool        // indicates whether a pre-alloc is in progress
}

// NewRW TODO
func NewRW(cb *block, options *Options) (b Block, err error) {
	segmentCount := cb.metadata.Segments
	segments := make([]*mmap.Map, segmentCount)

	wb := &rwblock{
		block:      cb,
		segments:   segments,
		addMutex:   &sync.Mutex{},
		allocMutex: &sync.Mutex{},
	}

	var i uint32
	for i = 0; i < segmentCount; i++ {
		segments[i], err = wb.loadSegment(i)
		if err != nil {
			Logger.Trace(err)
			return nil, err
		}
	}

	err = wb.preallocateIfNeeded()
	if err != nil {
		Logger.Trace(err)
		return nil, err
	}

	return wb, nil
}

func (b *rwblock) Add() (id uint32, err error) {
	// force run allocation if we've already run out of space
	if b.availableRecordSpace() <= 0 {
		b.allocMutex.Lock()

		if b.availableRecordSpace() <= 0 {
			err = b.allocateSegment()
			if err != nil {
				b.allocMutex.Unlock()
				Logger.Trace(err)
				return 0, err
			}

			b.metadata.Segments++
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
	nextID := b.metadata.Records
	b.metadata.Records++
	b.addMutex.Unlock()

	err = b.mdstore.Save()
	if err != nil {
		Logger.Trace(err)
		return 0, err
	}

	atomic.AddInt64(&b.metrics.AddOps, 1)
	return nextID, nil
}

func (b *rwblock) Put(id, pos uint32, pld []byte) (err error) {
	if pos > b.options.RSize || pos < 0 {
		Logger.Trace(ErrBound)
		return ErrBound
	}

	if uint32(len(pld)) != b.options.PSize {
		Logger.Trace(ErrPSize)
		return ErrPSize
	}

	payloadSize := b.options.PSize
	segmentSize := b.metadata.Capacity
	segmentNumber := id / segmentSize

	if segmentNumber >= b.metadata.Segments {
		Logger.Trace(ErrNoSeg)
		return ErrNoSeg
	}

	// record position inside the segment
	recordSize := payloadSize * b.options.RSize
	recordPosition := id % segmentSize
	offset := int64(recordPosition*recordSize + pos*b.options.PSize)
	mfile := b.segments[segmentNumber]

	n, err := mfile.WriteAt(pld, offset)
	if err != nil {
		Logger.Trace(err)
		return err
	} else if n != len(pld) {
		Logger.Trace(ErrWrite)
		return ErrWrite
	}

	atomic.AddInt64(&b.metrics.PutOps, 1)
	return nil
}

func (b *rwblock) Get(id, start, end uint32) (res [][]byte, err error) {
	if end > b.options.RSize || start < 0 {
		Logger.Trace(ErrBound)
		return nil, ErrBound
	}

	payloadSize := b.options.PSize
	segmentSize := b.metadata.Capacity
	segmentNumber := id / segmentSize

	if segmentNumber >= b.metadata.Segments {
		Logger.Trace(ErrNoSeg)
		return nil, ErrNoSeg
	}

	// record position inside the segment
	recordSize := payloadSize * b.options.RSize
	recordPosition := id % segmentSize
	startOffset := int64(recordPosition*recordSize + start*b.options.PSize)
	mfile := b.segments[segmentNumber]

	seriesLength := end - start
	seriesSize := seriesLength * b.options.PSize

	seriesData := make([]byte, seriesSize)
	n, err := mfile.ReadAt(seriesData, startOffset)
	if err != nil {
		return nil, err
	} else if uint32(n) != seriesSize {
		return nil, ErrRead
	}

	res = make([][]byte, seriesLength)

	var i uint32
	for i = 0; i < seriesLength; i++ {
		res[i] = seriesData[i*b.options.PSize : (i+1)*b.options.PSize]
	}

	atomic.AddInt64(&b.metrics.GetOps, 1)
	return res, nil
}

func (b *rwblock) Close() (err error) {
	err = b.block.Close()
	if err != nil {
		Logger.Trace(err)
		return err
	}

	for _, seg := range b.segments {
		err = seg.Close()
		if err != nil {
			Logger.Trace(err)
			return err
		}
	}

	return nil
}

func (b *rwblock) loadSegment(id uint32) (mfile *mmap.Map, err error) {
	istr := strconv.Itoa(int(id))
	fpath := path.Join(b.options.Path, SegPrefix+istr)
	payloadSize := b.options.PSize
	recordSize := payloadSize * b.options.RSize
	size := int64(b.metadata.Capacity * recordSize)

	mfile, err = mmap.New(&mmap.Options{Path: fpath, Size: size})
	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&b.metrics.Mapped, size)

	err = mfile.Lock()
	if err != nil {
		Logger.Error(err)
	}

	atomic.AddInt64(&b.metrics.Locked, size)

	return mfile, nil
}

func (b *rwblock) availableRecordSpace() (n uint32) {
	total := b.metadata.Segments * b.metadata.Capacity
	return total - b.metadata.Records
}

func (b *rwblock) preallocateIfNeeded() (err error) {
	if b.availableRecordSpace() < PreallocThresh {
		b.allocMutex.Lock()
		defer b.allocMutex.Unlock()

		if b.availableRecordSpace() < PreallocThresh {
			err = b.allocateSegment()
			if err != nil {
				b.allocating = false
				Logger.Trace(err)
				return err
			}

			b.metadata.Segments++
		}
	}

	b.allocating = false
	return nil
}

func (b *rwblock) allocateSegment() (err error) {
	mfile, err := b.loadSegment(b.metadata.Segments)
	if err != nil {
		Logger.Trace(err)
		return err
	}

	b.segments = append(b.segments, mfile)

	return nil
}
