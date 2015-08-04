package mdata

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/kadirahq/kadiyadb/utils/logger"
	"github.com/kadirahq/kadiyadb/utils/mmap"
)

const (
	// LoggerPrefix will be used to prefix debug logs
	LoggerPrefix = "MDATA"
)

var (
	// ErrWrite is returned when number of bytes doesn't match data size
	ErrWrite = errors.New("number of bytes written doesn't match data size")

	// ErrRead is returned when number of bytes doesn't match data size
	ErrRead = errors.New("number of bytes read doesn't match data size")

	// ErrROnly is returned when a save is requested on a read only mdata
	ErrROnly = errors.New("cannot save a read only mdata")
)

// Data is a protocol buffer message persisted in the disk
// Data is used by KadiyaDB to store metadata to the disk.
type Data interface {
	Save() (err error)
	Load() (err error)
	Close() (err error)
}

type mdata struct {
	proto proto.Message
	mfile *mmap.Map
	mutex *sync.Mutex
	ronly bool
	dbuff []byte

	loading bool
	doLoad  bool
	saving  bool
	doSave  bool
}

// New creates a new protocol buffer encoded message store saved on disk.
// The data will be memory mapped and stored in the disk when updated.
func New(path string, pb proto.Message, ro bool) (d Data, err error) {
	mfile, err := mmap.New(&mmap.Options{Path: path})
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	err = mfile.Lock()
	if err != nil {
		logger.Log(LoggerPrefix, err)
	}

	pp := &mdata{
		proto: pb,
		mfile: mfile,
		mutex: &sync.Mutex{},
		ronly: ro,
		dbuff: make([]byte, 0),
	}

	err = pp.load()
	if err != nil {
		logger.Log(LoggerPrefix, err)

		err = mfile.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
		}

		return nil, err
	}

	if ro {
		err = mfile.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
		}
	}

	return pp, err
}

func (d *mdata) Load() (err error) {
	if d.loading {
		d.doLoad = true
		return nil
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.loading = true

	err = d.load()
	if err != nil {
		d.loading = false
		logger.Log(LoggerPrefix, err)
		return err
	}

	if d.doLoad {
		d.doLoad = false
		err = d.load()
		if err != nil {
			d.loading = false
			logger.Log(LoggerPrefix, err)
			return err
		}
	}

	d.loading = false

	return nil
}

func (d *mdata) Save() (err error) {
	if d.ronly {
		return ErrROnly
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.saving = true

	err = d.save()
	if err != nil {
		d.saving = false
		logger.Log(LoggerPrefix, err)
		return err
	}

	if d.doSave {
		d.doSave = false
		err = d.save()
		if err != nil {
			d.saving = false
			logger.Log(LoggerPrefix, err)
			return err
		}
	}

	d.saving = false

	return nil
}

func (d *mdata) Close() (err error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.doSave {
		d.saving = true
		err = d.save()
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}
	}

	if d.ronly {
		return nil
	}

	err = d.mfile.Close()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}

func (d *mdata) load() (err error) {
	d.mfile.Reset()

	var sz uint32
	err = binary.Read(d.mfile, binary.LittleEndian, &sz)
	if err == io.EOF {
		return nil
	} else if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	currentSz := uint32(len(d.dbuff))
	if currentSz < sz {
		d.dbuff = make([]byte, sz)
	}

	n, err := d.mfile.Read(d.dbuff)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	} else if uint32(n) != sz {
		logger.Log(LoggerPrefix, ErrRead)
		return ErrRead
	}

	err = proto.Unmarshal(d.dbuff, d.proto)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}

func (d *mdata) save() (err error) {
	data, err := proto.Marshal(d.proto)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	d.mfile.Reset()

	dataSize := len(data)
	binary.Write(d.mfile, binary.LittleEndian, uint32(dataSize))
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	n, err := d.mfile.Write(data)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	} else if n != dataSize {
		logger.Log(LoggerPrefix, ErrWrite)
		return ErrWrite
	}

	return nil
}
