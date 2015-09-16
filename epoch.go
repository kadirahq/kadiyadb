package kadiyadb

import (
	"encoding/binary"
	"errors"
	"os"
	"path"
	"time"

	goerr "github.com/go-errors/errors"
	"github.com/kadirahq/go-tools/fnutils"
	"github.com/kadirahq/go-tools/mmap"
	"github.com/kadirahq/kadiyadb/block"
	"github.com/kadirahq/kadiyadb/index"
)

const (
	// IndexFileName is the name of the index file created inside epoch directory
	IndexFileName = "index"

	// UpdatedFileName is the name of the file to store last updated time
	UpdatedFileName = "updated"
)

var (
	// ErrNoEpoch is returned when the epoch is not found on disk (read-only).
	ErrNoEpoch = errors.New("requested epoch is not available on disk")
)

// EpochOptions has parameters required for creating a `Epoch`
type EpochOptions struct {
	Path string // directory to store index and block files

	// block options
	PSize uint32 // size of payload (point) in bytes
	RSize uint32 // number of payloads in a record
	SSize uint32 // nmber of records in a segment
	ROnly bool   // read only or read/write block
}

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

	// Sync synchronizes writes
	Sync() (err error)

	// Close cleans up stuff, releases resources and closes the epoch.
	Close() (err error)
}

type epoch struct {
	closed  bool
	options *EpochOptions // options
	index   index.Index   // index for the epoch
	block   block.Block   // block store for the epoch
	times   *mmap.File
	timesfn *fnutils.Group // update times
}

// NewEpoch creates an new `Epoch` with given `Options`
// If a epoch does not exist, it will be created.
func NewEpoch(options *EpochOptions) (_e Epoch, err error) {
	Monitor.Track("NewEpoch", 1)
	defer Logger.Time(time.Now(), time.Second, "NewEpoch")

	if options.ROnly {
		err = os.Chdir(options.Path)
		if err != nil {
			return nil, goerr.Wrap(ErrNoEpoch, 0)
		}
	}

	idxPath := path.Join(options.Path, IndexFileName)
	idxOptions := &index.Options{
		Path:  idxPath,
		ROnly: options.ROnly,
	}

	idx, err := index.New(idxOptions)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
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
		return nil, goerr.Wrap(err, 0)
	}

	tpath := path.Join(options.Path, UpdatedFileName)
	tim, err := mmap.NewFile(tpath, 10, true)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	tfn := fnutils.NewGroup(func() {
		now := time.Now().Unix()
		tim.Reset()
		if err := binary.Write(tim, binary.LittleEndian, now); err != nil {
			Logger.Error(err)
		}
	})

	e := &epoch{
		index:   idx,
		block:   blk,
		times:   tim,
		timesfn: tfn,
		options: options,
	}

	go func() {
		for {
			if e.closed {
				return
			}

			tfn.Flush()
			time.Sleep(time.Second)
		}
	}()

	return e, nil
}

func (e *epoch) Put(pos uint32, fields []string, value []byte) (err error) {
	Monitor.Track("epoch.Put", 1)
	defer Logger.Time(time.Now(), time.Second, "epoch.Put")

	if pos > e.options.RSize || pos < 0 {
		return block.ErrBound
	}

	for i := 1; i <= len(fields); i++ {
		flds := fields[:i]

		var rid uint32
		item, err := e.index.One(flds)
		if err == nil {
			rid = item.Value
		} else if goerr.Is(err, index.ErrNoItem) {
			id, err := e.block.Add()
			if err != nil {
				return goerr.Wrap(err, 0)
			}

			err = e.index.Put(flds, id)
			if err != nil && !goerr.Is(err, index.ErrExists) {
				return goerr.Wrap(err, 0)
			}

			rid = id
		} else {
			return goerr.Wrap(err, 0)
		}

		err = e.block.Put(rid, pos, value)
		if err != nil {
			return goerr.Wrap(err, 0)
		}
	}

	return nil
}

func (e *epoch) One(start, end uint32, fields []string) (out [][]byte, err error) {
	Monitor.Track("epoch.One", 1)
	defer Logger.Time(time.Now(), time.Second, "epoch.One")

	item, err := e.index.One(fields)
	if goerr.Is(err, index.ErrNoItem) {
		num := end - start
		out := make([][]byte, num)
		for i := range out {
			out[i] = make([]byte, e.options.PSize)
		}

		return out, nil
	}

	out, err = e.block.Get(item.Value, start, end)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	return out, nil
}

func (e *epoch) Get(start, end uint32, fields []string) (out map[*index.Item][][]byte, err error) {
	Monitor.Track("epoch.Get", 1)
	defer Logger.Time(time.Now(), time.Second, "epoch.Get")

	fast := true
	for _, v := range fields {
		if v == "" {
			fast = false
			break
		}
	}

	if fast {
		item, err := e.index.One(fields)
		if err != nil {
			return nil, goerr.Wrap(err, 0)
		}

		out = make(map[*index.Item][][]byte)
		out[item], err = e.block.Get(item.Value, start, end)
		if err != nil {
			return nil, goerr.Wrap(err, 0)
		}
	}

	items, err := e.index.Get(fields)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	out = make(map[*index.Item][][]byte)
	for _, item := range items {
		out[item], err = e.block.Get(item.Value, start, end)
		if err != nil {
			return nil, goerr.Wrap(err, 0)
		}
	}

	return out, nil
}

func (e *epoch) Sync() (err error) {
	Monitor.Track("epoch.Sync", 1)
	defer Logger.Time(time.Now(), time.Second, "epoch.Sync")

	err = e.index.Sync()
	if err != nil {
		return goerr.Wrap(err, 0)
	}

	err = e.block.Sync()
	if err != nil {
		return goerr.Wrap(err, 0)
	}

	return nil
}

func (e *epoch) Close() (err error) {
	Monitor.Track("epoch.Close", 1)
	defer Logger.Time(time.Now(), time.Second, "epoch.Close")
	e.closed = true

	err = e.index.Close()
	if err != nil {
		return goerr.Wrap(err, 0)
	}

	err = e.block.Close()
	if err != nil {
		return goerr.Wrap(err, 0)
	}

	err = e.times.Close()
	if err != nil {
		return goerr.Wrap(err, 0)
	}

	return nil
}
