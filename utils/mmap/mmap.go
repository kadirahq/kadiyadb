package mmap

import (
	"errors"
	"io"
	"os"
	"path"
	"sync"
	"syscall"

	"github.com/kadirahq/kadiyadb/utils/logger"
)

const (
	// LoggerPrefix will be used to prefix debug logs
	LoggerPrefix = "MMAP"

	// FileMode used when opening files for memory mapping
	FileMode = os.O_CREATE | os.O_RDWR

	// DirPerm is the permission values set when creating new directories
	DirPerm = 0755

	// FilePerm is the permissions used when creating new files
	FilePerm = 0644

	// FileProt is the memory map prot parameter
	FileProt = syscall.PROT_READ | syscall.PROT_WRITE

	// FileFlag is the memory map flag parameter
	FileFlag = syscall.MAP_SHARED

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
	Path string // memory map file path
	Size int64  // minimum size of the mmap file
}

// Map contains a memory map to a file
// TODO: mapping only a part of the file (consider page size)
type Map struct {
	opts *Options      // options
	data []byte        // mapped data
	size int64         // map size
	file *os.File      // map file
	lock bool          // whether the map is locked or not
	mutx *sync.RWMutex // read/write mutex
	roff int64         // io.Reader read offset
	woff int64         // io.Reader write offset
}

// New function creates a memory maps the file in given path
func New(options *Options) (m *Map, err error) {
	dpath := path.Dir(options.Path)
	err = os.MkdirAll(dpath, DirPerm)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	file, err := os.OpenFile(options.Path, FileMode, FilePerm)
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
		data: data,
		size: size,
		file: file,
		mutx: &sync.RWMutex{},
	}

	return m, nil
}

// Size returns the size of the memory map
func (m *Map) Size() (sz int64) {
	m.mutx.RLock()
	defer m.mutx.RUnlock()
	return m.size
}

// ReadAt reads a slice of bytes from the memory map at an offset
func (m *Map) ReadAt(p []byte, off int64) (n int, err error) {
	m.mutx.RLock()
	defer m.mutx.RUnlock()
	return m.read(p, off)
}

// WriteAt writes a slice of bytes to the memory map at an offset
// automatically grows the memory map when it runs out of space
func (m *Map) WriteAt(p []byte, off int64) (n int, err error) {
	m.mutx.Lock()
	defer m.mutx.Unlock()
	return m.write(p, off)
}

// Read reads a slice of bytes from the memory map
func (m *Map) Read(p []byte) (n int, err error) {
	m.mutx.RLock()
	defer m.mutx.RUnlock()

	n, err = m.read(p, m.roff)
	if err == nil {
		m.roff += int64(n)
	}

	return n, err
}

// Write writes a slice of bytes to the memory map
func (m *Map) Write(p []byte) (n int, err error) {
	m.mutx.Lock()
	defer m.mutx.Unlock()

	n, err = m.write(p, m.woff)
	if err == nil {
		m.woff += int64(n)
	}

	return n, err
}

// Reset resets io.Reader, io.Writer offsets
func (m *Map) Reset() {
	m.mutx.Lock()
	defer m.mutx.Unlock()

	m.roff = 0
	m.woff = 0
}

// Grow method grows the file by `size` number of bytes. Once it's done, the
// file will be re-mapped with added bytes.
func (m *Map) Grow(size int64) (err error) {
	m.mutx.Lock()
	defer m.mutx.Unlock()
	return m.grow(size)
}

// Lock method loads memory mapped data to the RAM and keeps them in RAM.
// If not done, the data will be kept on disk until required.
// Locking a memory map can decrease initial page faults.
func (m *Map) Lock() (err error) {
	if m.lock {
		return nil
	}

	err = mlock(m.data)
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
	if !m.lock {
		return nil
	}

	err = munlock(m.data)
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

	err = munmap(m.data)
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

func (m *Map) read(p []byte, off int64) (n int, err error) {
	var src []byte
	var end = off + int64(len(p))

	if end > m.size {
		err = io.EOF
		src = m.data[off:m.size]
		n = int(m.size - off)
	} else {
		src = m.data[off:end]
		n = int(end - off)
	}

	copy(p, src)
	return n, err
}

func (m *Map) write(p []byte, off int64) (n int, err error) {
	var dst []byte
	var end = off + int64(len(p))

	if end > m.size {
		toGrow := end - m.size
		err = m.grow(toGrow)
		if err != nil {
			return 0, err
		}
	}

	dst = m.data[off:end]
	n = int(end - off)
	copy(dst, p)
	return n, nil
}

func (m *Map) grow(size int64) (err error) {
	lock := m.lock

	if lock {
		err := m.Unlock()
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}

		m.lock = false
	}

	err = munmap(m.data)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	err = grow(m.file, size, m.size)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	m.size += size
	m.data, err = mmap(m.file, 0, m.size)
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

// grow grows a file with `size` number of bytes.
// `fsize` is the current file size in bytes.
// empty bytes are appended to the end of the file.
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

	return syscall.Mmap(fd, from, ln, FileProt, FileFlag)
}

// munmap unmaps mapped data
// If the data size is zero, a map cannot exist
// therefore assume no errors and return nil
func munmap(data []byte) (err error) {
	if len(data) == 0 {
		return nil
	}

	return syscall.Munmap(data)
}

// mlock locks data to physical memory
func mlock(data []byte) (err error) {
	return syscall.Mlock(data)
}

// munlock releases locked memory space
func munlock(data []byte) (err error) {
	return syscall.Munlock(data)
}
