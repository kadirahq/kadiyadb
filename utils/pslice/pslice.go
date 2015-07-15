package pslice

import (
	"errors"
	"reflect"
	"unsafe"

	"github.com/meteorhacks/kadiradb-core/utils/logger"
	"github.com/meteorhacks/kadiradb-core/utils/mmap"
)

const (
	// LoggerPrefix will be used to prefix debug logs
	LoggerPrefix = "PSLICE"

	// ItemSize is the number of bytes required to store an element in slice
	ItemSize = 8
)

var (
	// ErrEmpty is returned when user tries to create a Slice
	// with size set to zero (or not set in options which means zero)
	ErrEmpty = errors.New("cannot create empty slice")
)

// Slice is an int64 slice which is persisted to a file on disk. it uses a
// memory map to store slice data therefore, < 20ns latency can be expected.
// Issues: data file may not work across different platforms/architectures.
type Slice interface {
	// Put sets the value at index `i`
	Put(i int64, el int64) (err error)

	// Get returns value at position `i`
	Get(i int64) (el int64, err error)

	// Close cleans up data and releases resources
	Close() (err error)
}

type slice struct {
	opts     *Options       // options
	mmapFile *mmap.Map      // memory map
	pointer  unsafe.Pointer // pointer to mmapped data
	data     []int64        // mapped data
}

// Options has parameters required for creating a `Slice`
type Options struct {
	Path string
	Size int64
}

// New function creates a persistent slice in given path
func New(options *Options) (_s Slice, err error) {
	mfile, err := mmap.New(&mmap.Options{Path: options.Path})
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	if options.Size <= 0 {
		logger.Log(LoggerPrefix, ErrEmpty)
		return nil, ErrEmpty
	}

	err = mfile.Grow(options.Size * ItemSize)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	// pointer to mmap start
	pointer := unsafe.Pointer(&mfile.Data[0])

	// set data slice to use mmapped space
	data := make([]int64, options.Size)
	header := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	header.Data = (uintptr)(pointer)

	s := &slice{
		opts:     options,
		mmapFile: mfile,
		pointer:  pointer,
		data:     data,
	}

	return s, nil
}

func (s *slice) Put(i int64, el int64) (err error) {
	s.data[i] = el
	return nil
}

func (s *slice) Get(i int64) (el int64, err error) {
	el = s.data[i]
	return el, nil
}

func (s *slice) Close() (err error) {
	err = s.mmapFile.Close()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	s.pointer = nil
	s.data = nil

	return nil
}
