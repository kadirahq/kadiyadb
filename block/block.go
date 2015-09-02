package block

import (
	"errors"
	"os"
	"sync"
	"time"

	goerr "github.com/go-errors/errors"
	"github.com/kadirahq/go-tools/logger"
	"github.com/kadirahq/go-tools/monitor"
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
	Logger = logger.New("block")

	// Monitor collects runtime metrics
	Monitor = monitor.New("block")
)

func init() {
	Monitor.Register("New", monitor.Counter)
	Monitor.Register("block.Add", monitor.Counter)
	Monitor.Register("block.Put", monitor.Counter)
	Monitor.Register("block.Get", monitor.Counter)
	Monitor.Register("block.Sync", monitor.Counter)
	Monitor.Register("block.Close", monitor.Counter)
}

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

	// Sync synchronizes writes
	Sync() (err error)

	// Close cleans up stuff, releases resources and closes the block.
	Close() (err error)
}

// Metrics contains runtime metrics
type Metrics struct {
	// TODO code!
}

type block struct {
	// segmented file with block data
	data segfile.File

	// locks to ensure safe read/write
	// lock one series at a time
	locks []sync.RWMutex

	// add mutex to control adds
	mutex sync.Mutex

	// log with block info
	logger *logger.Logger

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
	Monitor.Track("New", 1)
	defer Logger.Time(time.Now(), time.Second, "New")

	// validate options
	if options == nil ||
		options.Path == "" ||
		options.PSize <= 0 ||
		options.RSize <= 0 ||
		options.SSize <= 0 {
		return nil, goerr.Wrap(ErrOptions, 0)
	}

	if options.ROnly {
		err = os.Chdir(options.Path)

		if os.IsNotExist(err) {
			err = ErrNoBlock
		}

		if err != nil {
			return nil, goerr.Wrap(err, 0)
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
		return nil, goerr.Wrap(err, 0)
	}

	if sf.Size()%int64(options.RSize*options.PSize) != 0 {
		return nil, goerr.Wrap(ErrCorrupt, 0)
	}

	// number of available
	records := sf.Size() / int64(options.RSize*options.PSize)
	locks := make([]sync.RWMutex, records)

	b = &block{
		data:   sf,
		locks:  locks,
		ronly:  options.ROnly,
		psize:  options.PSize,
		rsize:  options.RSize,
		logger: Logger.New(options.Path),
	}

	return b, nil
}

func (b *block) Add() (id uint32, err error) {
	Monitor.Track("block.Add", 1)
	defer Logger.Time(time.Now(), time.Second, "block.Add")

	if b.ronly {
		return 0, goerr.Wrap(ErrROnly, 0)
	}

	rsize := int64(b.rsize * b.psize)

	b.mutex.Lock()
	defer b.mutex.Unlock()

	dsize := b.data.Size()

	if dsize%rsize != 0 {
		return 0, goerr.Wrap(ErrCorrupt, 0)
	}

	id = uint32(dsize / rsize)

	err = b.data.Grow(rsize)
	if err != nil {
		return 0, goerr.Wrap(err, 0)
	}

	// add another lock
	b.locks = append(b.locks, sync.RWMutex{})

	return id, nil
}

func (b *block) Put(id, pos uint32, pld []byte) (err error) {
	Monitor.Track("block.Put", 1)
	defer Logger.Time(time.Now(), time.Second, "block.Put")

	if b.ronly {
		return goerr.Wrap(ErrROnly, 0)
	}

	if pos >= b.rsize || pos < 0 {
		return goerr.Wrap(ErrBound, 0)
	}

	if len(pld) != int(b.psize) {
		return goerr.Wrap(ErrPSize, 0)
	}

	dsize := b.data.Size()
	rsize := int64(b.rsize * b.psize)

	if dsize%rsize != 0 {
		return goerr.Wrap(ErrCorrupt, 0)
	}

	nextID := uint32(dsize / rsize)
	if id >= nextID {
		return goerr.Wrap(ErrNoRec, 0)
	}

	lock := b.locks[id]
	lock.Lock()
	defer lock.Unlock()

	roff := id * b.rsize * b.psize
	offset := int64(roff + pos*b.psize)

	n, err := b.data.WriteAt(pld, offset)
	if err != nil {
		return goerr.Wrap(err, 0)
	} else if n != int(b.psize) {
		return goerr.Wrap(ErrWrite, 0)
	}

	return nil
}

func (b *block) Get(id, start, end uint32) (res [][]byte, err error) {
	Monitor.Track("block.Get", 1)
	defer Logger.Time(time.Now(), time.Second, "block.Get")

	if end > b.rsize || start < 0 || end < start {
		return nil, goerr.Wrap(ErrBound, 0)
	}

	count := end - start
	size := count * b.psize
	data := make([]byte, size)
	roff := id * b.rsize * b.psize
	offset := int64(roff + start*b.psize)

	if id < uint32(len(b.locks)) {
		lock := b.locks[id]
		lock.RLock()
		defer lock.RUnlock()
	}

	n, err := b.data.ReadAt(data, offset)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	} else if uint32(n) != size {
		return nil, goerr.Wrap(ErrRead, 0)
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

func (b *block) Sync() (err error) {
	Monitor.Track("block.Sync", 1)
	defer Logger.Time(time.Now(), time.Second, "block.Sync")

	if b.closed {
		b.logger.Error(ErrClose)
		return nil
	}

	if b.ronly {
		return nil
	}

	err = b.data.Sync()
	if err != nil {
		return goerr.Wrap(err, 0)
	}

	return nil
}

func (b *block) Close() (err error) {
	Monitor.Track("block.Close", 1)
	defer Logger.Time(time.Now(), time.Second, "block.Close")

	if b.closed {
		b.logger.Error(ErrClose)
		return nil
	}

	err = b.data.Close()
	if err != nil {
		return goerr.Wrap(err, 0)
	}

	return nil
}
