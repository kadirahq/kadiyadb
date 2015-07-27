package kdb

import (
	"os"
	"path"

	"github.com/kadirahq/kadiradb-core/block"
	"github.com/kadirahq/kadiradb-core/index"
	"github.com/kadirahq/kadiradb-core/utils/logger"
)

const (
	// IndexFileName is the name of the index file created inside epoch directory
	IndexFileName = "index"
)

// Epoch contains an index and a block store for fixed a time period.
// The purpose of separating into terms is to keep the record size static.
// Terms can also be deleted without affecting other terms. It also helps to
// clean up the index tree (ignores old index values when creating a new epoch)
type Epoch interface {
	// Put saves a data point into the database.
	// It'll add a record and an index entry if necessary.
	Put(pos uint32, fields []string, value []byte) (err error)

	// One gets a single specific result series from the database
	One(start, end uint32, fields []string) (out [][]byte, err error)

	// Get gets a series of data points from the database
	Get(start, end uint32, fields []string) (out map[*index.Item][][]byte, err error)

	// Close cleans up stuff, releases resources and closes the epoch.
	Close() (err error)
}

type epoch struct {
	opts *EpochOptions // options
	idx  index.Index   // index for the epoch
	blk  block.Block   // block store for the epoch
}

// EpochOptions has parameters required for creating a `Epoch`
type EpochOptions struct {
	Path string // directory to store index and block files

	// block options
	PSize uint32 // size of payload (point) in bytes
	RSize uint32 // number of payloads in a record
	SSize uint32 // nmber of records in a segment
	ROnly bool   // read only or read/write block
}

// NewEpoch creates an new `Epoch` with given `Options`
// If a epoch does not exist, it will be created.
func NewEpoch(options *EpochOptions) (_e Epoch, err error) {
	if options.ROnly {
		err = os.Chdir(options.Path)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return nil, err
		}
	}

	idxPath := path.Join(options.Path, IndexFileName)
	idxOptions := &index.Options{
		Path:  idxPath,
		ROnly: options.ROnly,
	}

	idx, err := index.New(idxOptions)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	blkOptions := &block.Options{
		Path:  options.Path,
		PSize: options.PSize,
		RSize: options.RSize,
		SSize: options.SSize,
		ROnly: options.ROnly,
	}

	blk, err := block.New(blkOptions)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	e := &epoch{
		idx:  idx,
		blk:  blk,
		opts: options,
	}

	return e, nil
}

func (e *epoch) Put(pos uint32, fields []string, value []byte) (err error) {
	if pos > e.opts.RSize || pos < 0 {
		logger.Log(LoggerPrefix, block.ErrBound)
		return block.ErrBound
	}

	var recordID uint32

	item, err := e.idx.One(fields)
	if err == nil {
		recordID = item.Value
	} else if err == index.ErrNoItem {
		id, err := e.blk.Add()
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}

		err = e.idx.Put(fields, id)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}

		recordID = id
	} else {
		logger.Log(LoggerPrefix, err)
		return err
	}

	err = e.blk.Put(recordID, pos, value)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}

func (e *epoch) One(start, end uint32, fields []string) (out [][]byte, err error) {
	item, err := e.idx.One(fields)
	if err == index.ErrNoItem {
		num := end - start
		out := make([][]byte, num)
		for i := range out {
			out[i] = make([]byte, e.opts.PSize)
		}

		return out, nil
	}

	out, err = e.blk.Get(item.Value, start, end)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	return out, nil
}

func (e *epoch) Get(start, end uint32, fields []string) (out map[*index.Item][][]byte, err error) {
	out = make(map[*index.Item][][]byte)

	items, err := e.idx.Get(fields)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	for _, item := range items {
		out[item], err = e.blk.Get(item.Value, start, end)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return nil, err
		}
	}

	return out, nil
}

func (e *epoch) Close() (err error) {
	err = e.idx.Close()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	err = e.blk.Close()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}
