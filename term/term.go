package term

import (
	"bytes"
	"encoding/binary"
	"os"
	"path"

	"github.com/meteorhacks/kadiradb-core/block"
	"github.com/meteorhacks/kadiradb-core/index"
	"github.com/meteorhacks/kadiradb-core/utils/logger"
)

const (
	// LoggerPrefix will be used to prefix debug logs
	LoggerPrefix = "TERM"
	// IndexFileName is the name of the index file created inside term directory
	IndexFileName = "index"
)

// Term contains an index and a block store for fixed a time period.
// The purpose of separating into terms is to keep the record size static.
// Terms can also be deleted without affecting other terms. It also helps to
// clean up the index tree (ignores old index values when creating a new term)
type Term interface {
	// Put saves a data point into the database.
	// It'll add a record and an index entry if necessary.
	Put(pos int64, fields []string, value []byte) (err error)

	// Get gets a series of data points from the database
	Get(start, end int64, fields []string) (out map[*index.Item][][]byte, err error)

	// Close cleans up stuff, releases resources and closes the term.
	Close() (err error)
}

type term struct {
	opts *Options    // options
	idx  index.Index // index for the term
	blk  block.Block // block store for the term
}

// Options has parameters required for creating a `Term`
type Options struct {
	Path string // directory to store index and block files

	// block options
	PayloadSize   int64 // size of payload (point) in bytes
	PayloadCount  int64 // number of payloads in a record
	SegmentLength int64 // nmber of records in a segment
	ReadOnly      bool  // read only or read/write block
}

// New creates an new `Term` with given `Options`
// If a term does not exist, it will be created.
func New(options *Options) (_t Term, err error) {
	idxPath := path.Join(options.Path, IndexFileName)
	idx, err := index.New(&index.Options{Path: idxPath})
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	blkOptions := &block.Options{
		Path:          options.Path,
		PayloadSize:   options.PayloadSize,
		PayloadCount:  options.PayloadCount,
		SegmentLength: options.SegmentLength,
		ReadOnly:      options.ReadOnly,
	}

	blk, err := block.New(blkOptions)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	t := &term{
		idx:  idx,
		blk:  blk,
		opts: options,
	}

	return t, nil
}

// Open opens an new `Term` with given `Options`
// It will return an error if the term doesn't exist.
func Open(options *Options) (_t Term, err error) {
	err = os.Chdir(options.Path)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	return New(options)
}

func (t *term) Put(pos int64, fields []string, value []byte) (err error) {
	if pos > t.opts.PayloadCount || pos < 0 {
		logger.Log(LoggerPrefix, block.ErrOutOfBounds)
		return block.ErrOutOfBounds
	}

	var recordID int64

	item, err := t.idx.One(fields)
	if err == nil {
		recordID, err = decodeInt64(item.Value)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}
	} else if err == index.ErrNotFound {
		id, err := t.blk.Add()
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}

		val, err := encodeInt64(id)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}

		err = t.idx.Put(fields, val)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}

		recordID = id
	} else {
		logger.Log(LoggerPrefix, err)
		return err
	}

	err = t.blk.Put(recordID, pos, value)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}

func (t *term) Get(start, end int64, fields []string) (out map[*index.Item][][]byte, err error) {
	out = make(map[*index.Item][][]byte)

	items, err := t.idx.Get(fields)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	for _, item := range items {
		id, err := decodeInt64(item.Value)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return nil, err
		}

		out[item], err = t.blk.Get(id, start, end)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return nil, err
		}
	}

	return out, nil
}

func (t *term) Close() (err error) {
	err = t.idx.Close()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	err = t.blk.Close()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}

func encodeInt64(n int64) (b []byte, err error) {
	buf := bytes.NewBuffer(nil)
	err = binary.Write(buf, binary.LittleEndian, n)
	if err != nil {
		return nil, err
	}

	b = buf.Bytes()
	return b, nil
}

func decodeInt64(b []byte) (n int64, err error) {
	buf := bytes.NewBuffer(b)
	err = binary.Read(buf, binary.LittleEndian, &n)
	if err != nil {
		return 0, err
	}

	return n, nil
}
