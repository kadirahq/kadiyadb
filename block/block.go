package block

import (
	"errors"
	"os"
	"path"
	"sync/atomic"

	"github.com/kadirahq/kadiyadb/utils/logger"
	"github.com/kadirahq/kadiyadb/utils/mdata"
)

const (
	// LoggerPrefix will be used to prefix debug logs
	LoggerPrefix = "BLOCK"

	// SegPrefix is the name prefixed to each segment file.
	// Segment files are numbered from 0 e.g. "seg_0, seg_1, seg_2, ..."
	SegPrefix = "seg_"

	// MDFileName is the metadata file which has segment information.
	// This file is stored in same directory with all segment files.
	MDFileName = "metadata"

	// MDHeaderSz is the size of the header info stored with metadata
	// Currently the header only contains a single `uint32` to store the size.
	MDHeaderSz = 4

	// PreallocThresh is the minimum number record space to keep available
	// to have ready. Additional segments will be created automatically.
	PreallocThresh = 1000
)

var (
	// ErrWrite is returned when number of bytes doesn't match data size
	ErrWrite = errors.New("number of bytes written doesn't match data size")

	// ErrRead is returned when number of bytes doesn't match data size
	ErrRead = errors.New("number of bytes read doesn't match data size")

	// ErrBound is returned when requested start,end times are invalid
	ErrBound = errors.New("requested start/end times are invalid")

	// ErrROnly is returned when a write is requested on a read only block
	ErrROnly = errors.New("cannot write on a read only block")

	// ErrNoBlock is returned when the block is not found on disk (read-only).
	ErrNoBlock = errors.New("requested block is not available on disk")

	// ErrPSize is returned when payload size doesn't match block options
	ErrPSize = errors.New("payload size is not compatible with block")
)

// Options has parameters required for creating a block
type Options struct {
	Path  string // files stored under this directory
	PSize uint32 // number of bye
	RSize uint32 // number of payloads in a record
	SSize uint32 // number of records in a segment
	ROnly bool   // read only or read/write block
}

// Block is a collection of records which contains a series of fixed sized
// binary payloads. Records are partitioned into segments in order to speed up
// disk space allocation.
type Block interface {
	// Add creates a new record in a segment file.
	// If there's no space, a new segment file will be created.
	Add() (id uint32, err error)

	// Put saves a data point into the database.
	Put(id, pos uint32, pld []byte) (err error)

	// Get gets a series of data points from the database
	Get(id, start, end uint32) (res [][]byte, err error)

	// Metrics returns performance metrics
	// It also resets all counters
	Metrics() (m *Metrics)

	// Close cleans up stuff, releases resources and closes the block.
	Close() (err error)
}

type block struct {
	options  *Options   // options
	metadata *Metadata  // metadata contains segment details
	mdstore  mdata.Data // persistence helper for metadata
	metrics  *Metrics   // performance metrics
}

// New creates a `Block` to store or get (time) series data.
// The `ROnly` option determines whether it'll be a read-only (roblock)
// or a writable (rwblock). It also loads metadata from disk if available.
// For read-only blocks, it'll test whether the block exists by checking
// whether the directory of the block exists and program can access it.
func New(options *Options) (b Block, err error) {
	if options.ROnly {
		err = os.Chdir(options.Path)

		if os.IsNotExist(err) {
			err = ErrNoBlock
		}

		if err != nil {
			logger.Log(LoggerPrefix, err)
			return nil, err
		}
	}

	metadata := &Metadata{
		Capacity: options.SSize,
		Segments: 0,
		Records:  0,
	}

	mdpath := path.Join(options.Path, MDFileName)
	mdstore, err := mdata.New(mdpath, metadata, options.ROnly)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	cb := &block{
		options:  options,
		metadata: metadata,
		metrics:  &Metrics{},
	}

	if options.ROnly {
		err = mdstore.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
		}

		return NewRO(cb, options)
	}

	cb.mdstore = mdstore
	return NewRW(cb, options)
}

func (b *block) Metrics() (m *Metrics) {
	metrics := *b.metrics
	metrics.Capacity = int64(b.metadata.Capacity)
	metrics.Segments = int64(b.metadata.Segments)
	metrics.Records = int64(b.metadata.Records)
	atomic.StoreInt64(&b.metrics.AddOps, 0)
	atomic.StoreInt64(&b.metrics.PutOps, 0)
	atomic.StoreInt64(&b.metrics.GetOps, 0)
	return &metrics
}

func (b *block) Close() (err error) {
	if b.mdstore != nil {
		err = b.mdstore.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}
	}

	return nil
}
