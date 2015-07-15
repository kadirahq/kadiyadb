package mmap

import (
	"errors"
	"os"
	"path"
	"sync"
	"syscall"

	"github.com/meteorhacks/kadiradb-core/utils/logger"
)

// Memory mapping parameters
const (
	MemmapFileMode = os.O_CREATE | os.O_RDWR
	MemmapDirPerm  = 0755
	MemmapFilePerm = 0644
	MemmapFileProt = syscall.PROT_READ | syscall.PROT_WRITE
	MemmapFileFlag = syscall.MAP_SHARED
)

const (
	// LoggerPrefix will be used to prefix debug logs
	LoggerPrefix = "MMAP"

	// AllocChunkSize is the number of bytes to write at a time
	AllocChunkSize = 1024 * 1024 * 10
)

var (
	// ErrWrite is returned when number of bytes doesn't match data size
	ErrWrite = errors.New("number of bytes written doesn't match data size")

	// ChunkBytes is a AllocChunkSize size slice of zeroes
	ChunkBytes = make([]byte, AllocChunkSize, AllocChunkSize)
)

// Options has parameters required for creating an `Index`
type Options struct {
	Path string
	Size int64
}

// Map contains a memory map to a file
// TODO: mapping only a part of the file (consider page size)
type Map struct {
	opts *Options
	Data []byte
	Size int64
	file *os.File
	lock bool
	mutx sync.Mutex
}

// New function creates a memory maps the file in given path
func New(options *Options) (m *Map, err error) {
	dpath := path.Dir(options.Path)
	err = os.MkdirAll(dpath, MemmapDirPerm)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	file, err := os.OpenFile(options.Path, MemmapFileMode, MemmapFilePerm)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	finfo, err := file.Stat()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	size := finfo.Size()

	if toGrow := options.Size - size; toGrow > 0 {
		err = grow(file, toGrow, size)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return nil, err
		}

		size = options.Size
	}

	data, err := mmap(file, 0, size)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	m = &Map{
		opts: options,
		Data: data,
		Size: size,
		file: file,
		mutx: sync.Mutex{},
	}

	return m, nil
}

// Grow method grows the file by `size` number of bytes. Once it's done, the
// file will be re-mapped with added bytes.
func (m *Map) Grow(size int64) (err error) {
	m.mutx.Lock()
	defer m.mutx.Unlock()

	lock := m.lock

	if lock {
		err := m.Unlock()
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}

		m.lock = false
	}

	err = munmap(m.Data)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	err = grow(m.file, size, m.Size)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	m.Size += size
	m.Data, err = mmap(m.file, 0, m.Size)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	if lock {
		err := m.Lock()
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}

		m.lock = true
	}

	return nil
}

// Lock method loads memory mapped data to the RAM and keeps them in RAM.
// If not done, the data will be kept on disk until required.
// Locking a memory map can decrease initial page faults.
func (m *Map) Lock() (err error) {
	m.mutx.Lock()
	defer m.mutx.Unlock()

	if m.lock {
		return nil
	}

	err = mlock(m.Data)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	m.lock = true
	return nil
}

// Unlock method releases memory by not reserving parts of RAM of the file.
// The operating system may use memory mapped data from the disk when done.
func (m *Map) Unlock() (err error) {
	m.mutx.Lock()
	defer m.mutx.Unlock()

	if !m.lock {
		return nil
	}

	err = munlock(m.Data)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	m.lock = false
	return nil
}

// Close method unmaps data and closes the file.
// If the mmap is locked, it'll be unlocked first.
func (m *Map) Close() (err error) {
	err = m.Unlock()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	m.mutx.Lock()
	defer m.mutx.Unlock()

	err = munmap(m.Data)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	err = m.file.Close()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}

func grow(file *os.File, size, fsize int64) (err error) {
	// number of complete chunks to write
	chunksCount := size / AllocChunkSize

	var i int64
	for i = 0; i < chunksCount; i++ {
		offset := fsize + AllocChunkSize*i
		n, err := file.WriteAt(ChunkBytes, offset)
		if err != nil {
			return err
		} else if int64(n) != AllocChunkSize {
			return ErrWrite
		}
	}

	// write all remaining bytes
	toWrite := size % AllocChunkSize
	zeroes := ChunkBytes[:toWrite]
	offset := fsize + AllocChunkSize*chunksCount
	n, err := file.WriteAt(zeroes, offset)
	if err != nil {
		return err
	} else if int64(n) != toWrite {
		return ErrWrite
	}

	return nil
}

// mmap function creates a new memory map for the given file.
// if the file size is zero, a memory cannot be created therefore
// an empty byte array is returned instead.
func mmap(file *os.File, from, to int64) (data []byte, err error) {
	fd := int(file.Fd())
	ln := int(to - from)

	if ln == 0 {
		data = make([]byte, 0, 0)
		return data, nil
	}

	return syscall.Mmap(fd, from, ln, MemmapFileProt, MemmapFileFlag)
}

// If the data size is zero, a map cannot exist
func munmap(data []byte) (err error) {
	if len(data) == 0 {
		return nil
	}

	return syscall.Munmap(data)
}

func mlock(data []byte) (err error) {
	return syscall.Mlock(data)
}

func munlock(data []byte) (err error) {
	return syscall.Munlock(data)
}
