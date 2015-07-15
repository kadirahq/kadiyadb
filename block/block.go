package block

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/meteorhacks/kadiradb-core/utils/logger"
	"github.com/meteorhacks/kadiradb-core/utils/mmap"
)

// Segment file parameters. This is used with read-only blocks where we
// read from and write to files directly instead of using a memory map
const (
	SegmentOpenMode    = os.O_CREATE | os.O_RDWR
	SegmentPermissions = 0644
)

const (
	// LoggerPrefix will be used to prefix debug logs
	LoggerPrefix = "BLOCK"

	// ReadOnly blocks does not use memory mapping therefore it can be
	// slower compared to memory mapped blocks. But they use much less memory
	// than memory mapped blocks.
	ReadOnly = 0

	// ReadWrite blocks use memory mapping to handle fast write and read
	// performance. But uses more memory than ReadOnly blocks.
	ReadWrite = false

	// MetadataHeaderSize is the number of bytes stored used to store size
	// with each Metadata (protobuf).
	MetadataHeaderSize = 8
)

var (
	// ErrWrite is returned when number of bytes doesn't match data size
	ErrWrite = errors.New("number of bytes written doesn't match data size")
	// ErrRead is returned when number of bytes doesn't match data size
	ErrRead = errors.New("number of bytes read doesn't match data size")
	// ErrOutOfBounds is returned when requested start,end times are invalid
	ErrOutOfBounds = errors.New("number of bytes read doesn't match data size")
	// ErrReadOnly is returned when a write is requested on a read only block
	ErrReadOnly = errors.New("cannot write on a read only block")
	// ErrWrongSize is returned when payload is given with wrong length
	ErrWrongSize = errors.New("payload size is not compatible with block")
)

// Options has parameters required for creating a block
type Options struct {
	Path          string // files stored under this directory
	PayloadSize   int64  // size of payload (point) in bytes
	PayloadCount  int64  // number of payloads in a record
	SegmentLength int64  // nmber of records in a segment
	ReadOnly      bool   // read only or read/write block
}

// Block is a collection of records which contains a series of fixed sized
// binary payloads. Records are partitioned into segments in order to speed up
// disk space allocation.
type Block interface {
	// Add creates a new record in a segment file.
	// If there's no space, a new segment file will be created.
	Add() (id int64, err error)

	// Put saves a data point into the database.
	Put(id, pos int64, pld []byte) (err error)

	// Get gets a series of data points from the database
	Get(id, start, end int64) (res [][]byte, err error)

	// Close cleans up stuff, releases resources and closes the block.
	Close() (err error)
}

type block struct {
	opts         *Options
	recordSize   int64         // total size of a record
	recordCount  int64         // number of records in a segment
	metadata     *Metadata     // metadata contains information about segments
	metadataMap  *mmap.Map     // memory map of metadata file
	metadataMutx *sync.Mutex   // mutex to control metadata writes
	metadataBuff *bytes.Buffer // reuseable buffer for saving metadata
}

// New creates a `Block` to store or get (time) series data.
// The `ReadOnly` option determines whether it'll be a read-only (roblock)
// or a writable (rwblock). It also loads metadata from disk if available
func New(options *Options) (blk Block, err error) {
	metadataPath := path.Join(options.Path, "metadata")
	metadataMap, err := mmap.New(&mmap.Options{Path: metadataPath})
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	b := &block{
		opts:         options,
		recordSize:   options.PayloadSize * options.PayloadCount,
		metadata:     &Metadata{SegmentLength: options.SegmentLength},
		metadataMap:  metadataMap,
		metadataMutx: &sync.Mutex{},
		metadataBuff: bytes.NewBuffer(nil),
	}

	err = b.loadMetadata()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		if err := metadataMap.Close(); err != nil {
			logger.Log(LoggerPrefix, err)
		}

		return nil, err
	}

	if options.ReadOnly {
		blk, err = newROBlock(b, options)
	} else {
		blk, err = newRWBlock(b, options)
	}

	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	return blk, nil
}

func (b *block) Close() (err error) {
	err = b.metadataMap.Close()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}

func (b *block) loadMetadata() (err error) {
	buff := bytes.NewBuffer(b.metadataMap.Data)

	var size int64
	err = binary.Read(buff, binary.LittleEndian, &size)
	if err == io.EOF {
		return nil
	} else if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	data := make([]byte, size)
	n, err := buff.Read(data)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	} else if int64(n) != size {
		logger.Log(LoggerPrefix, ErrRead)
		return ErrRead
	}

	err = proto.Unmarshal(data, b.metadata)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}

func (b *block) saveMetadata() (err error) {
	if b.opts.ReadOnly {
		logger.Log(LoggerPrefix, ErrReadOnly)
		return ErrReadOnly
	}

	data, err := proto.Marshal(b.metadata)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	b.metadataMutx.Lock()
	defer b.metadataMutx.Unlock()

	// create a Writer in order to use binary.Write
	b.metadataBuff.Reset()
	buff := b.metadataBuff

	dataSize := int64(len(data))
	binary.Write(buff, binary.LittleEndian, dataSize)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	n, err := buff.Write(data)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	} else if int64(n) != dataSize {
		logger.Log(LoggerPrefix, ErrWrite)
		return ErrWrite
	}

	totalSize := dataSize + MetadataHeaderSize
	if b.metadataMap.Size < totalSize {
		toGrow := totalSize - b.metadataMap.Size
		err = b.metadataMap.Grow(toGrow)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}
	}

	src := buff.Bytes()
	dst := b.metadataMap.Data
	for i, d := range src {
		dst[i] = d
	}

	return nil
}
