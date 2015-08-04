package kadiyadb

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/kadirahq/kadiyadb/index"
	"github.com/kadirahq/kadiyadb/utils/clock"
	"github.com/kadirahq/kadiyadb/utils/logger"
	"github.com/kadirahq/kadiyadb/utils/mdata"
)

const (
	// LoggerPrefix will be used to prefix debug logs
	LoggerPrefix = "KDB"

	// EpochPrefix will be prefixed to each epoch directory
	// e.g. epoch_0, epoch_10, ... (if epoch duration is 10)
	EpochPrefix = "epoch_"

	// MDFileName is the name of the metadata file.
	// This file is stored with segment files in same directory.
	MDFileName = "metadata"
)

var (
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

	// ErrExists is returned when a database already exists at given path
	ErrExists = errors.New("path for new database already exists")
)

// Options has parameters required for creating a `Database`
type Options struct {
	Path        string // directory to store epochs
	Resolution  int64  // resolution as a string
	Duration    int64  // duration of a single epoch
	PayloadSize uint32 // size of payload (point) in bytes
	SegmentSize uint32 // number of records in a segment
	MaxROEpochs uint32 // maximum read-only buckets (uses file handlers)
	MaxRWEpochs uint32 // maximum read-write buckets (uses memory maps)
	Recovery    bool   // load the db in recovery mode (always rw epochs)
}

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

	// Info returns database metadata
	Info() (metadata *Metadata)

	// Edit updates metadata
	Edit(metadata *Metadata) (err error)

	// Metrics returns performance metrics
	// It also resets all counters
	Metrics() (m *Metrics)

	// Close cleans up stuff, releases resources and closes the database.
	Close() (err error)
}

type database struct {
	metadata *Metadata   // metadata contains segment details
	mdstore  mdata.Data  // persistence helper for metadata
	roepochs Cache       // a cache to hold read-only epochs
	rwepochs Cache       // a cache to hold read-write epochs
	mdMutex  *sync.Mutex // mutex to control metadata changes
	epoMutex *sync.Mutex // mutex to control opening closing epochs
	recovery bool        // always use read-write epochs
}

// New creates an new `Database` with given `Options`
// Although options are stored in
func New(options *Options) (db Database, err error) {
	logger.Debug(LoggerPrefix, "Create: '"+options.Path+"'")

	err = os.Chdir(options.Path)
	if err == nil {
		logger.Log(LoggerPrefix, ErrExists)
		return nil, ErrExists
	}

	if options.Duration%options.Resolution != 0 {
		logger.Log(LoggerPrefix, ErrDurRes)
		return nil, ErrDurRes
	}

	// evictFn is called when the lru cache runs out of space
	evictFn := func(k int64, epo Epoch) {
		err := epo.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
		}
	}

	roepochs := NewCache(int(options.MaxROEpochs), evictFn)
	rwepochs := NewCache(int(options.MaxRWEpochs), evictFn)

	metadata := &Metadata{
		Path:        options.Path,
		Resolution:  options.Resolution,
		Duration:    options.Duration,
		PayloadSize: options.PayloadSize,
		SegmentSize: options.SegmentSize,
		MaxROEpochs: options.MaxROEpochs,
		MaxRWEpochs: options.MaxRWEpochs,
	}

	mdpath := path.Join(options.Path, MDFileName)
	mdstore, err := mdata.New(mdpath, metadata, false)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	err = mdstore.Save()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	db = &database{
		metadata: metadata,
		mdstore:  mdstore,
		roepochs: roepochs,
		rwepochs: rwepochs,
		mdMutex:  &sync.Mutex{},
		epoMutex: &sync.Mutex{},
		recovery: options.Recovery,
	}

	return db, nil
}

// Open opens an existing database from the disk
func Open(dbpath string, recovery bool) (db Database, err error) {
	logger.Debug(LoggerPrefix, "Open: '"+dbpath+"'")

	metadata := &Metadata{}
	mdpath := path.Join(dbpath, MDFileName)
	mdstore, err := mdata.New(mdpath, metadata, true)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	// evictFn is called when the cache leaks
	evictFn := func(k int64, epo Epoch) {
		err := epo.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
		}
	}

	roepochs := NewCache(int(metadata.MaxROEpochs), evictFn)
	rwepochs := NewCache(int(metadata.MaxRWEpochs), evictFn)

	db = &database{
		metadata: metadata,
		mdstore:  mdstore,
		roepochs: roepochs,
		rwepochs: rwepochs,
		mdMutex:  &sync.Mutex{},
		epoMutex: &sync.Mutex{},
		recovery: recovery,
	}

	return db, nil
}

func (db *database) Put(ts int64, fields []string, value []byte) (err error) {
	md := db.metadata
	dur := md.Duration
	res := md.Resolution

	// floor ts to a point start time
	ts -= ts % res

	epo, err := db.getEpoch(ts)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	trmStart := ts - (ts % dur)
	pos := uint32((ts - trmStart) / res)

	err = epo.Put(pos, fields, value)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}

func (db *database) One(start, end int64, fields []string) (out [][]byte, err error) {
	md := db.metadata
	dur := md.Duration
	res := md.Resolution

	// floor ts to a point start time
	start -= start % res
	end -= end % res

	if end <= start {
		return nil, ErrRange
	}

	epoFirst := start - (start % dur)
	epoLast := end - (end % dur)
	pcount := (end - start) / res

	out = make([][]byte, pcount)

	var trmStart, trmEnd int64

	for ts := epoFirst; ts <= epoLast; ts += dur {
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
			trmEnd = ts + dur
		}

		numPoints := (trmEnd - trmStart) / res
		startPos := uint32((trmStart % dur) / res)
		endPos := startPos + uint32(numPoints)
		result, err := epo.One(startPos, endPos, fields)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			continue
		}

		recStart := (trmStart - start) / res
		recEnd := (trmEnd - start) / res
		copy(out[recStart:recEnd], result)
	}

	return out, nil
}

func (db *database) Get(start, end int64, fields []string) (out map[*index.Item][][]byte, err error) {
	md := db.metadata
	dur := md.Duration
	res := md.Resolution

	// floor ts to a point start time
	start -= start % res
	end -= end % res

	if end <= start {
		return nil, ErrRange
	}

	epoFirst := start - (start % dur)
	epoLast := end - (end % dur)
	pointCount := (end - start) / res

	tmpPoints := make(map[string][][]byte)
	tmpFields := make(map[string][]string)

	var trmStart, trmEnd int64

	for ts := epoFirst; ts <= epoLast; ts += dur {
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
			trmEnd = ts + dur
		}

		numPoints := uint32((trmEnd - trmStart) / res)
		startPos := uint32((trmStart % dur) / res)
		endPos := startPos + numPoints
		result, err := epo.Get(startPos, endPos, fields)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			continue
		}

		for item, points := range result {
			// TODO: use a better way to identify fieldsets
			// on rare occassions can cause incorect result
			// build a temporary tree for accurate results
			key := strings.Join(item.Fields, `-`)
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

			recStart := (trmStart - start) / res
			recEnd := (trmEnd - start) / res
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
	md := db.metadata
	dur := md.Duration

	// floor ts to a epoch start time
	ts -= ts % dur

	now := clock.Now()
	now -= now % dur
	min := now - int64(db.metadata.MaxRWEpochs-1)*dur

	if ts >= min {
		return ErrRWEpoch
	}

	files, err := ioutil.ReadDir(db.metadata.Path)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	for _, finfo := range files {
		fname := finfo.Name()
		if !strings.HasPrefix(fname, EpochPrefix) {
			continue
		}

		tsStr := strings.TrimPrefix(fname, EpochPrefix)
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

		bpath := path.Join(db.metadata.Path, fname)
		err = os.RemoveAll(bpath)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			continue
		}
	}

	return nil
}

func (db *database) Info() (metadata *Metadata) {
	return db.metadata
}

func (db *database) Edit(metadata *Metadata) (err error) {
	logger.Log(LoggerPrefix, "Edit: '"+db.Info().Path+"'", metadata)

	db.mdMutex.Lock()
	defer db.mdMutex.Unlock()

	if metadata.MaxROEpochs != 0 {
		db.metadata.MaxROEpochs = metadata.MaxROEpochs
		db.roepochs.Resize(int(db.metadata.MaxROEpochs))
	}

	if metadata.MaxRWEpochs != 0 {
		db.metadata.MaxRWEpochs = metadata.MaxRWEpochs
		db.rwepochs.Resize(int(db.metadata.MaxRWEpochs))
	}

	return db.mdstore.Save()
}

func (db *database) Metrics() (m *Metrics) {
	roepochs := db.roepochs.Data()
	rometrics := make(map[int64]*EpochMetrics)
	for k, e := range roepochs {
		rometrics[k] = e.Metrics()
	}

	rwepochs := db.rwepochs.Data()
	rwmetrics := make(map[int64]*EpochMetrics)
	for k, e := range rwepochs {
		rwmetrics[k] = e.Metrics()
	}

	return &Metrics{
		REpochs: rometrics,
		WEpochs: rwmetrics,
	}
}

func (db *database) Close() (err error) {
	// Purge will send all epochs to the evict function.
	// The evict function is set inside the New function.
	// epochs will be properly closed there.
	db.roepochs.Purge()
	db.rwepochs.Purge()

	err = db.mdstore.Close()
	if err != nil {
		return err
	}

	return nil
}

// getEpoch loads a epoch into memory and returns it
// if ro is true, loads the epoch in read-only mode
func (db *database) getEpoch(ts int64) (epo Epoch, err error) {
	md := db.metadata

	// floor ts to a epoch start time
	ts -= ts % md.Duration

	now := clock.Now()
	now -= now % md.Duration
	min := now - int64(md.MaxRWEpochs-1)*md.Duration
	max := now + md.Duration

	if ts >= max {
		return nil, ErrFuture
	}

	// decide whether we need a read-only or read-write epoch
	// present epoch is also included when calculating `min`
	ro := ts < min

	// Forces loading epochs in rw mode when in recovery mode.
	// This can be useful for writing data to the disk later
	// when the epoch is not loaded for writes by deafult.
	if db.recovery {
		ro = false
	}

	var epochs Cache
	if ro {
		epochs = db.roepochs
	} else {
		epochs = db.rwepochs
	}

	epo, ok := epochs.Get(ts)
	if ok {
		return epo, nil
	}

	payloadCount := uint32(md.Duration / md.Resolution)

	istr := strconv.Itoa(int(ts))
	tpath := path.Join(md.Path, EpochPrefix+istr)
	options := &EpochOptions{
		Path:  tpath,
		PSize: md.PayloadSize,
		RSize: payloadCount,
		SSize: md.SegmentSize,
		ROnly: ro,
	}

	epo, err = NewEpoch(options)
	if err != nil {
		return nil, err
	}

	epochs.Add(ts, epo)

	return epo, nil
}
