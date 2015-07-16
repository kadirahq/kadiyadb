package kdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/golang-lru"
	"github.com/meteorhacks/kadiradb-core/index"
	"github.com/meteorhacks/kadiradb-core/utils/logger"
	"github.com/meteorhacks/kadiradb-core/utils/mmap"
	"github.com/meteorhacks/kdb/clock"
)

const (
	// LoggerPrefix will be used to prefix debug logs
	LoggerPrefix = "KDB"

	// EpochDirPrefix will be prefixed to each epoch directory
	// e.g. epoch_0, epoch_10, ... (if epoch duration is 10)
	EpochDirPrefix = "epoch_"

	// MetadataFileName is the name of the metadata file.
	// This file is stored with segment files in same directory.
	MetadataFileName = "metadata"

	// MetadataHeaderSize is the number of bytes stored used to store size
	// with each Metadata (protobuf).
	MetadataHeaderSize = 8
)

var (
	// ErrRead is returned when number of bytes doesn't match data size
	ErrRead = errors.New("number of bytes read doesn't match data size")
	// ErrWrite is returned when number of bytes doesn't match data size
	ErrWrite = errors.New("number of bytes written doesn't match data size")
	// ErrDurRes is returned when given duration is not a multiple of resolution
	// Each point in a epoch represents a `resolution` amount of time (in ns).
	ErrDurRes = errors.New("duration should be a multiple of resolution")
	// ErrFuture is returned when user requests data form a future epoch
	// It is also returned when user tries to Put data for a future timestamp.
	ErrFuture = errors.New("timestamp is set to a future time")
	// ErrRWEpoch is returned when user tries to remove a read-write epoch
	ErrRWEpoch = errors.New("cannot delete read-write epochs")
	// ErrRange is returned when thegiven range is not valid
	ErrRange = errors.New("provided time range is not valid")
	// ErrMetadata is returned when metadata doesn't match db options
	ErrMetadata = errors.New("db options doesn't match metadata")
	// ErrExists is returned when a database already exists at given path
	ErrExists = errors.New("path for new database already exists")
)

// Database is a time series database which can store fixed sized payloads.
// Data can be queried using dynamic number of fields with specific value
// or wildcard values (only supports "" for match-all at the moment).
type Database interface {
	// Put stores data in the database for specific timestamp and set of fields
	Put(ts int64, fields []string, value []byte) (err error)

	// Get gets a series of data points from the database
	// Data can be taken from one or more `epochs`.
	Get(start, end int64, fields []string) (out map[*index.Item][][]byte, err error)

	// One gets a single series of data points from the database
	// Data can be taken from one or more `epochs`.
	One(start, end int64, fields []string) (out [][]byte, err error)

	// Expire removes all epochs before given timestamp
	Expire(ts int64) (err error)

	// Metadata returns database metadata
	Metadata() (metadata *Metadata)

	// Close cleans up stuff, releases resources and closes the database.
	Close() (err error)
}

// Options has parameters required for creating a `Database`
type Options struct {
	BasePath      string // directory to store epochs
	Resolution    int64  // resolution as a string
	EpochDuration int64  // duration of a single epoch
	PayloadSize   int64  // size of payload (point) in bytes
	SegmentLength int64  // number of records in a segment
	MaxROEpochs   int64  // maximum read-only buckets (uses file handlers)
	MaxRWEpochs   int64  // maximum read-write buckets (uses memory maps)
}

type database struct {
	roepochs     *lru.Cache    // a cache to hold read-only epochs
	rwepochs     *lru.Cache    // a cache to hold read-write epochs
	epoMutex     *sync.Mutex   // mutex to control opening closing epochs
	metadata     *Metadata     // metadata contains information about segments
	metadataMap  *mmap.Map     // memory map of metadata file
	metadataMutx *sync.Mutex   // mutex to control metadata writes
	metadataBuff *bytes.Buffer // reuseable buffer for saving metadata
}

// New creates an new `Database` with given `Options`
// Although options are stored in
func New(options *Options) (_db Database, err error) {
	log.Println(LoggerPrefix, "Create new database '"+options.BasePath+"'")
	err = os.Chdir(options.BasePath)
	if err == nil {
		logger.Log(LoggerPrefix, ErrExists)
		return nil, ErrExists
	}

	if options.EpochDuration%options.Resolution != 0 {
		logger.Log(LoggerPrefix, ErrDurRes)
		return nil, ErrDurRes
	}

	// evictFn is called when the lru cache runs out of space
	evictFn := func(k interface{}, v interface{}) {
		epo := v.(Epoch)
		err := epo.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
		}
	}

	roepochs, err := lru.NewWithEvict(int(options.MaxROEpochs), evictFn)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	rwepochs, err := lru.NewWithEvict(int(options.MaxRWEpochs), evictFn)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	metadataPath := path.Join(options.BasePath, MetadataFileName)
	metadataMap, err := mmap.New(&mmap.Options{Path: metadataPath})
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	db := &database{
		roepochs:     roepochs,
		rwepochs:     rwepochs,
		epoMutex:     &sync.Mutex{},
		metadata:     &Metadata{},
		metadataMap:  metadataMap,
		metadataMutx: &sync.Mutex{},
		metadataBuff: bytes.NewBuffer(nil),
	}

	db.metadata = &Metadata{
		BasePath:      options.BasePath,
		Resolution:    options.Resolution,
		EpochDuration: options.EpochDuration,
		PayloadSize:   options.PayloadSize,
		SegmentLength: options.SegmentLength,
		MaxROEpochs:   options.MaxROEpochs,
		MaxRWEpochs:   options.MaxRWEpochs,
	}

	err = db.saveMetadata()
	if err != nil {
		logger.Log(LoggerPrefix, err)

		err = db.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
		}

		return nil, err
	}

	return db, nil
}

// Open opens an existing database from the disk
func Open(basePath string) (_db Database, err error) {
	log.Println(LoggerPrefix, "Open database '"+basePath+"'")
	metadataPath := path.Join(basePath, MetadataFileName)
	metadataMap, err := mmap.New(&mmap.Options{Path: metadataPath})
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	db := &database{
		epoMutex:     &sync.Mutex{},
		metadata:     &Metadata{},
		metadataMap:  metadataMap,
		metadataMutx: &sync.Mutex{},
		metadataBuff: bytes.NewBuffer(nil),
	}

	err = db.loadMetadata()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	// evictFn is called when the lru cache runs out of space
	evictFn := func(k interface{}, v interface{}) {
		epo := v.(Epoch)
		err := epo.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
		}
	}

	db.roepochs, err = lru.NewWithEvict(int(db.metadata.MaxROEpochs), evictFn)
	if err != nil {
		logger.Log(LoggerPrefix, err)

		err = db.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
		}

		return nil, err
	}

	db.rwepochs, err = lru.NewWithEvict(int(db.metadata.MaxRWEpochs), evictFn)
	if err != nil {
		logger.Log(LoggerPrefix, err)

		err = db.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
		}

		return nil, err
	}

	return db, nil
}

func (db *database) Put(ts int64, fields []string, value []byte) (err error) {
	// floor ts to a point start time
	ts -= ts % db.metadata.Resolution

	epo, err := db.getEpoch(ts)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	trmStart := ts - (ts % db.metadata.EpochDuration)
	pos := (ts - trmStart) / db.metadata.Resolution

	err = epo.Put(pos, fields, value)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}

func (db *database) One(start, end int64, fields []string) (out [][]byte, err error) {
	// floor ts to a point start time
	start -= start % db.metadata.Resolution
	end -= end % db.metadata.Resolution

	if end <= start {
		return nil, ErrRange
	}

	epoFirst := start - (start % db.metadata.EpochDuration)
	epoLast := end - (end % db.metadata.EpochDuration)
	pointCount := (end - start) / db.metadata.Resolution

	out = make([][]byte, pointCount)

	var trmStart, trmEnd int64

	for ts := epoFirst; ts <= epoLast; ts += db.metadata.EpochDuration {
		epo, err := db.getEpoch(ts)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			continue
		}

		// if it's the first bucket
		// skip payloads before `start` time
		// defaults to base time of the bucket
		if ts == epoFirst {
			trmStart = start
		} else {
			trmStart = ts
		}

		// if this is the last bucket
		// skip payloads after `end` time
		// defaults to end of the bucket
		if ts == epoLast {
			trmEnd = end
		} else {
			trmEnd = ts + db.metadata.EpochDuration
		}

		numPoints := (trmEnd - trmStart) / db.metadata.Resolution
		startPos := (trmStart % db.metadata.EpochDuration) / db.metadata.Resolution
		endPos := startPos + numPoints
		res, err := epo.One(startPos, endPos, fields)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			continue
		}

		recStart := (trmStart - start) / db.metadata.Resolution
		recEnd := (trmEnd - start) / db.metadata.Resolution
		copy(out[recStart:recEnd], res)
	}

	return out, nil
}

func (db *database) Get(start, end int64, fields []string) (out map[*index.Item][][]byte, err error) {
	// floor ts to a point start time
	start -= start % db.metadata.Resolution
	end -= end % db.metadata.Resolution

	if end <= start {
		return nil, ErrRange
	}

	epoFirst := start - (start % db.metadata.EpochDuration)
	epoLast := end - (end % db.metadata.EpochDuration)
	pointCount := (end - start) / db.metadata.Resolution

	tmpPoints := make(map[string][][]byte)
	tmpFields := make(map[string][]string)

	var trmStart, trmEnd int64

	for ts := epoFirst; ts <= epoLast; ts += db.metadata.EpochDuration {
		epo, err := db.getEpoch(ts)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			continue
		}

		// if it's the first bucket
		// skip payloads before `start` time
		// defaults to base time of the bucket
		if ts == epoFirst {
			trmStart = start
		} else {
			trmStart = ts
		}

		// if this is the last bucket
		// skip payloads after `end` time
		// defaults to end of the bucket
		if ts == epoLast {
			trmEnd = end
		} else {
			trmEnd = ts + db.metadata.EpochDuration
		}

		numPoints := (trmEnd - trmStart) / db.metadata.Resolution
		startPos := (trmStart % db.metadata.EpochDuration) / db.metadata.Resolution
		endPos := startPos + numPoints
		res, err := epo.Get(startPos, endPos, fields)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			continue
		}

		for item, points := range res {
			// TODO: use a better way to identify fieldsets
			key := strings.Join(item.Fields, `¯\\_(ツ)_/¯`)
			set, ok := tmpPoints[key]
			if !ok {
				set = make([][]byte, pointCount, pointCount)

				var i int64
				for i = 0; i < pointCount; i++ {
					set[i] = make([]byte, db.metadata.PayloadSize)
				}

				tmpPoints[key] = set
				tmpFields[key] = item.Fields
			}

			recStart := (trmStart - start) / db.metadata.Resolution
			recEnd := (trmEnd - start) / db.metadata.Resolution
			copy(set[recStart:recEnd], points)
		}
	}

	out = make(map[*index.Item][][]byte)
	for key, fields := range tmpFields {
		item := &index.Item{Fields: fields}
		out[item] = tmpPoints[key]
	}

	return out, nil
}

func (db *database) Expire(ts int64) (err error) {
	// floor ts to a epoch start time
	ts -= ts % db.metadata.EpochDuration

	now := clock.Now()
	now -= now % db.metadata.EpochDuration
	min := now - (db.metadata.MaxRWEpochs-1)*db.metadata.EpochDuration

	if ts >= min {
		return ErrRWEpoch
	}

	files, err := ioutil.ReadDir(db.metadata.BasePath)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	for _, finfo := range files {
		fname := finfo.Name()
		if !strings.HasPrefix(fname, EpochDirPrefix) {
			continue
		}

		tsStr := strings.TrimPrefix(fname, EpochDirPrefix)
		tsInt, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			continue
		}

		if tsInt >= ts {
			continue
		}

		v, ok := db.roepochs.Peek(tsInt)
		if ok {
			epo := v.(Epoch)
			err = epo.Close()
			if err != nil {
				logger.Log(LoggerPrefix, err)
				continue
			}
		}

		bpath := path.Join(db.metadata.BasePath, fname)
		err = os.RemoveAll(bpath)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			continue
		}
	}

	return nil
}

func (db *database) Metadata() (metadata *Metadata) {
	return db.metadata
}

func (db *database) Close() (err error) {
	// Purge will send all epochs to the evict function.
	// The evict function is set inside the New function.
	// epochs will be properly closed there.
	db.roepochs.Purge()
	db.rwepochs.Purge()

	err = db.metadataMap.Close()
	if err != nil {
		return err
	}

	return nil
}

// getEpoch loads a epoch into memory and returns it
// if ro is true, loads the epoch in read-only mode
func (db *database) getEpoch(ts int64) (epo Epoch, err error) {
	// floor ts to a epoch start time
	ts -= ts % db.metadata.EpochDuration

	now := clock.Now()
	now -= now % db.metadata.EpochDuration
	min := now - (db.metadata.MaxRWEpochs-1)*db.metadata.EpochDuration
	max := now + db.metadata.EpochDuration

	if ts >= max {
		return nil, ErrFuture
	}

	// decide whether we need a read-only or read-write epoch
	// present epoch is also included when calculating `min`
	ro := ts < min

	var epochs *lru.Cache
	if ro {
		epochs = db.roepochs
	} else {
		epochs = db.rwepochs
	}

	val, ok := epochs.Get(ts)
	if ok {
		epo = val.(Epoch)
		return epo, nil
	}

	payloadCount := db.metadata.EpochDuration / db.metadata.Resolution

	istr := strconv.Itoa(int(ts))
	tpath := path.Join(db.metadata.BasePath, EpochDirPrefix+istr)
	options := &EpochOptions{
		Path:          tpath,
		PayloadSize:   db.metadata.PayloadSize,
		PayloadCount:  payloadCount,
		SegmentLength: db.metadata.SegmentLength,
		ReadOnly:      ro,
	}

	epo, err = NewEpoch(options)
	if err != nil {
		return nil, err
	}

	epochs.Add(ts, epo)

	return epo, nil
}

// loadMetadata loads metadata info from disk encoded with protocol buffer.
// any metadata info set earlier will be replaced with those on the file
func (db *database) loadMetadata() (err error) {
	buff := bytes.NewBuffer(db.metadataMap.Data)

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

	err = proto.Unmarshal(data, db.metadata)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}

// saveMetadata encodes metadata info with protocol buffer and stores it in
// a file. The file is memory mapped to increase write performance.
// This function is run whenever a new record is added so it runs often.
func (db *database) saveMetadata() (err error) {
	data, err := proto.Marshal(db.metadata)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	db.metadataMutx.Lock()
	defer db.metadataMutx.Unlock()

	// create a Writer in order to use binary.Write
	db.metadataBuff.Reset()
	buff := db.metadataBuff

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
	if db.metadataMap.Size < totalSize {
		toGrow := totalSize - db.metadataMap.Size
		err = db.metadataMap.Grow(toGrow)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}
	}

	src := buff.Bytes()
	dst := db.metadataMap.Data
	for i, d := range src {
		dst[i] = d
	}

	return nil
}
