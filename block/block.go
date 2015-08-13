package block

import (
	"errors"
	"os"
	"sync"

	"github.com/kadirahq/go-tools/logger"
	"github.com/kadirahq/go-tools/segfile"
)

const (
	// FilePrefix will be prefixed to segment files
	FilePrefix = "block_"
)

var (
	// ErrClose is returned when close is closed multiple times
	ErrClose = errors.New("close called multiple times")

	// ErrWrite is returned when bytes written is not equal to data size
	ErrWrite = errors.New("bytes written != data size")

	// ErrRead is returned when bytes read is not equal to data size
	ErrRead = errors.New("bytes read != data size")

	// ErrOptions is returned when options have missing or invalid fields.
	ErrOptions = errors.New("invalid or missing options")

	// ErrBound is returned when requested start,end times are invalid
	ErrBound = errors.New("requested start/end times are invalid")

	// ErrROnly is returned when a write is requested on a read only block
	ErrROnly = errors.New("cannot write on a read only block")

	// ErrNoBlock is returned when the block is not found on disk (read-only).
	ErrNoBlock = errors.New("requested block is not available on disk")

	// ErrNoRec is returned when the record is not found on block
	ErrNoRec = errors.New("record is not found on block")

	// ErrPSize is returned when payload size doesn't match block options
	ErrPSize = errors.New("payload size is not compatible with block")

	// ErrCorrupt is returned when the segfile is corrupt
	ErrCorrupt = errors.New("segfile is corrupt")

	// Logger logs stuff
	Logger = logger.New("BLOCK")
)

// Options for new block
type Options struct {
	// directory to store block files
	Path string

	// number of bytes in a payload
	PSize uint32

	// number of payloads in a record
	RSize uint32

	// number of records in a segment
	SSize uint32

	// no writes allowed
	ROnly bool
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
	// segmented file with block data
	data segfile.File

	// add mutex to control adds
	mutex *sync.Mutex

	// set to true when the file is closed
	closed bool

	// set to true when the file is read only
	ronly bool

	// payload size
	psize uint32

	// record size
	rsize uint32
}

// New creates a `Block` to store or get (time) series data.
// It'll test whether the block exists by checking whether the directory
// of the block exists and whether the program has permissions to access it.
func New(options *Options) (b Block, err error) {
	// validate options
	if options == nil ||
		options.Path == "" ||
		options.PSize <= 0 ||
		options.RSize <= 0 ||
		options.SSize <= 0 {
		Logger.Trace(ErrOptions)
		return nil, ErrOptions
	}

	if options.ROnly {
		err = os.Chdir(options.Path)

		if os.IsNotExist(err) {
			err = ErrNoBlock
		}

		if err != nil {
			Logger.Trace(err)
			return nil, err
		}
	}

	ssize := options.SSize * options.RSize * options.PSize
	so := &segfile.Options{
		Path:      options.Path,
		Prefix:    FilePrefix,
		FileSize:  int64(ssize),
		ReadOnly:  options.ROnly,
		MemoryMap: !options.ROnly,
	}

	sf, err := segfile.New(so)
	if err != nil {
		Logger.Trace(err)
		return nil, err
	}

	meta := sf.Info()
	if meta.DataSize%int64(options.RSize*options.PSize) != 0 {
		Logger.Trace(ErrCorrupt)
		return nil, ErrCorrupt
	}

	b = &block{
		data:  sf,
		mutex: &sync.Mutex{},
		ronly: options.ROnly,
		psize: options.PSize,
		rsize: options.RSize,
	}

	return b, nil
}

func (b *block) Add() (id uint32, err error) {
	if b.ronly {
		Logger.Trace(ErrROnly)
		return 0, ErrROnly
	}

	rsize := int64(b.rsize * b.psize)

	b.mutex.Lock()
	defer b.mutex.Unlock()

	meta := b.data.Info()

	if meta.DataSize%rsize != 0 {
		Logger.Trace(ErrCorrupt)
		return 0, ErrCorrupt
	}

	id = uint32(meta.DataSize / rsize)

	err = b.data.Grow(rsize)
	if err != nil {
		Logger.Trace(err)
		return 0, err
	}

	return id, nil
}

func (b *block) Put(id, pos uint32, pld []byte) (err error) {
	if b.ronly {
		Logger.Trace(ErrROnly)
		return ErrROnly
	}

	if pos >= b.rsize || pos < 0 {
		Logger.Trace(ErrBound)
		return ErrBound
	}

	if len(pld) != int(b.psize) {
		Logger.Trace(ErrPSize)
		return ErrPSize
	}

	meta := b.data.Info()
	rsize := int64(b.rsize * b.psize)

	if meta.DataSize%rsize != 0 {
		Logger.Trace(ErrCorrupt)
		return ErrCorrupt
	}

	nextID := uint32(meta.DataSize / rsize)
	if id >= nextID {
		Logger.Trace(ErrNoRec)
		return ErrNoRec
	}

	roff := id * b.rsize * b.psize
	offset := int64(roff + pos*b.psize)

	n, err := b.data.WriteAt(pld, offset)
	if err != nil {
		Logger.Trace(err)
		return err
	} else if n != int(b.psize) {
		Logger.Trace(ErrWrite)
		return ErrWrite
	}

	return nil
}

func (b *block) Get(id, start, end uint32) (res [][]byte, err error) {
	if end > b.rsize || start < 0 || end < start {
		Logger.Trace(ErrBound)
		return nil, ErrBound
	}

	count := end - start
	size := count * b.psize
	data := make([]byte, size)
	roff := id * b.rsize * b.psize
	offset := int64(roff + start*b.psize)

	n, err := b.data.ReadAt(data, offset)
	if err != nil {
		Logger.Trace(err)
		return nil, err
	} else if uint32(n) != size {
		Logger.Trace(ErrRead)
		return nil, ErrRead
	}

	res = make([][]byte, count)

	var i uint32
	for i = 0; i < count; i++ {
		res[i] = data[i*b.psize : (i+1)*b.psize]
	}

	return res, nil
}

func (b *block) Metrics() (m *Metrics) {
	// TODO code!
	return &Metrics{}
}

func (b *block) Close() (err error) {
	if b.closed {
		Logger.Error(ErrClose)
		return nil
	}

	err = b.data.Close()
	if err != nil {
		Logger.Trace(err)
		return err
	}

	return nil
}
