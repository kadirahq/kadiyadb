package kadiyadb

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	goerr "github.com/go-errors/errors"
	"github.com/kadirahq/go-tools/logger"
	"github.com/kadirahq/go-tools/secure"
	"github.com/kadirahq/go-tools/vtimer"
	"github.com/kadirahq/kadiyadb/index"
)

const (
	// EpochPrefix will be prefixed to each epoch directory
	// e.g. epoch_0, epoch_10, ... (if epoch duration is 10)
	EpochPrefix = "epoch_"

	// MDFileName is the name of the metadata file.
	// This file is stored with segment files in same directory.
	MDFileName = "metadata"

	// RetInterval is the interval to check epoch retention
	RetInterval = time.Minute
)

var (
	// ErrWrite is returned when bytes written is not equal to data size
	ErrWrite = errors.New("bytes written != data size")

	// ErrRead is returned when bytes read is not equal to data size
	ErrRead = errors.New("bytes read != data size")

	// ErrOpts is returned when required options are missing or invalid
	// it's also is returned when given duration is not a multiple of resolution
	// Each point in a epoch represents a `resolution` amount of time (in ns).
	ErrOpts = errors.New("duration should be a multiple of resolution")

	// ErrFuture is returned when user requests data form a future epoch
	// It is also returned when user tries to Put data for a future timestamp.
	ErrFuture = errors.New("timestamp is set to a future time")

	// ErrRWEpoch is returned when user tries to remove a read-write epoch
	ErrRWEpoch = errors.New("cannot delete read-write epochs")

	// ErrRange is returned when thegiven range is not valid
	ErrRange = errors.New("provided time range is not valid")

	// ErrExists is returned when a database already exists at given path
	ErrExists = errors.New("path for new database already exists")

	// ErrMData is returned when metadata is invalid or corrupt
	ErrMData = errors.New("invalid or corrupt metadata")

	// ErrClosed is returned when using closed segfile
	ErrClosed = errors.New("cannot use closed database")

	// Jogger logs stuff
	Jogger = logger.New("kadiyadb")
)

// Options has parameters required for creating a `Database`
type Options struct {
	Path        string // directory to store epochs
	Resolution  int64  // resolution in nano seconds
	Retention   int64  // retention time in nano seconds
	Duration    int64  // duration of a single epoch in nano seconds
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

	// Info returns database info
	Info() (info *Info, err error)

	// Edit updates metadata
	Edit(mro, mrw uint32) (err error)

	// Metrics returns performance metrics
	// It also resets all counters
	Metrics() (m *Metrics, err error)

	// Sync synchronizes writes
	Sync() (err error)

	// Close cleans up stuff, releases resources and closes the database.
	Close() (err error)
}

// Metrics contains runtime metrics
type Metrics struct {
	// TODO code!
}

// Info has database information
type Info struct {
	Duration    int64
	Retention   int64
	Resolution  int64
	PayloadSize uint32
	SegmentSize uint32
	MaxROEpochs uint32
	MaxRWEpochs uint32
}

type database struct {
	metadata *Metadata      // metadata contains segment details
	roepochs Cache          // a cache to hold read-only epochs
	rwepochs Cache          // a cache to hold read-write epochs
	epoMutex sync.RWMutex   // mutex to control opening closing epochs
	recovery bool           // always use read-write epochs
	dbpath   string         // path to database files
	logger   *logger.Logger // log with db info
	closed   *secure.Bool   // indicates whether db is open/close
}

// New creates an new `Database` with given `Options`
// Although options are stored in
func New(options *Options) (db Database, err error) {
	if options.Path == "" ||
		options.Duration == 0 ||
		options.Retention == 0 ||
		options.Resolution == 0 ||
		options.PayloadSize == 0 ||
		options.SegmentSize == 0 ||
		options.MaxROEpochs == 0 ||
		options.MaxRWEpochs == 0 ||
		options.Duration%options.Resolution != 0 {
		return nil, goerr.Wrap(ErrOpts, 0)
	}

	if err := os.Chdir(options.Path); err == nil {
		return nil, goerr.Wrap(ErrExists, 0)
	}

	dblogger := Jogger.New(options.Path)

	// evictFn is called when the lru cache runs out of space
	evictFn := func(k int64, epo Epoch) {
		err := epo.Close()
		if err != nil {
			dblogger.Error(err)
		}
	}

	roepochs := NewCache(int(options.MaxROEpochs), evictFn)
	rwepochs := NewCache(int(options.MaxRWEpochs), evictFn)

	mdpath := path.Join(options.Path, MDFileName)
	mdata, err := NewMetadata(mdpath,
		options.Duration,
		options.Retention,
		options.Resolution,
		options.PayloadSize,
		options.SegmentSize,
		options.MaxROEpochs,
		options.MaxRWEpochs)

	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	if mdata.Duration() == 0 ||
		mdata.Retention() == 0 ||
		mdata.Resolution() == 0 ||
		mdata.PayloadSize() == 0 ||
		mdata.SegmentSize() == 0 ||
		mdata.MaxROEpochs() == 0 ||
		mdata.MaxRWEpochs() == 0 {
		return nil, goerr.Wrap(ErrMData, 0)
	}

	dbase := &database{
		metadata: mdata,
		roepochs: roepochs,
		rwepochs: rwepochs,
		recovery: options.Recovery,
		dbpath:   options.Path,
		closed:   secure.NewBool(false),
		logger:   dblogger,
	}

	// start the expire loop
	go dbase.enforceRetention()

	return dbase, nil
}

// Open opens an existing database from the disk
// if recovery mode bool is true, all epochs will be loaded with
// read-write capabilities instead of read-only for older epochs
func Open(dbpath string, recovery bool) (db Database, err error) {
	mdpath := path.Join(dbpath, MDFileName)
	mdata, err := ReadMetadata(mdpath)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	if mdata.Duration() == 0 ||
		mdata.Retention() == 0 ||
		mdata.Resolution() == 0 ||
		mdata.PayloadSize() == 0 ||
		mdata.SegmentSize() == 0 ||
		mdata.MaxROEpochs() == 0 ||
		mdata.MaxRWEpochs() == 0 {
		return nil, goerr.Wrap(ErrMData, 0)
	}

	dblogger := Jogger.New(dbpath)

	// evictFn is called when the lru cache runs out of space
	evictFn := func(k int64, epo Epoch) {
		err := epo.Close()
		if err != nil {
			dblogger.Error(err)
		}
	}

	roepochs := NewCache(int(mdata.MaxROEpochs()), evictFn)
	rwepochs := NewCache(int(mdata.MaxRWEpochs()), evictFn)

	dbase := &database{
		metadata: mdata,
		roepochs: roepochs,
		rwepochs: rwepochs,
		recovery: recovery,
		dbpath:   dbpath,
		closed:   secure.NewBool(false),
		logger:   dblogger,
	}

	// start the expire loop
	go dbase.enforceRetention()

	return dbase, nil
}

func (db *database) Info() (info *Info, err error) {
	if db.closed.Get() {
		return nil, goerr.Wrap(ErrClosed, 0)
	}

	db.metadata.RLock()
	defer db.metadata.RUnlock()

	info = &Info{
		Duration:    db.metadata.Duration(),
		Retention:   db.metadata.Retention(),
		Resolution:  db.metadata.Resolution(),
		PayloadSize: db.metadata.PayloadSize(),
		SegmentSize: db.metadata.SegmentSize(),
		MaxROEpochs: db.metadata.MaxROEpochs(),
		MaxRWEpochs: db.metadata.MaxRWEpochs(),
	}

	return info, nil
}

func (db *database) Edit(maxROEpochs, maxRWEpochs uint32) (err error) {
	if db.closed.Get() {
		return goerr.Wrap(ErrClosed, 0)
	}

	db.metadata.Lock()
	defer db.metadata.Unlock()

	if maxROEpochs != 0 {
		db.metadata.SetMaxROEpochs(maxROEpochs)
		db.roepochs.Resize(int(maxROEpochs))
	}

	if maxRWEpochs != 0 {
		db.metadata.SetMaxRWEpochs(maxRWEpochs)
		db.rwepochs.Resize(int(maxRWEpochs))
	}

	db.metadata.Sync()

	return nil
}

func (db *database) Metrics() (m *Metrics, err error) {
	if db.closed.Get() {
		return nil, goerr.Wrap(ErrClosed, 0)
	}

	// TODO collect metrics
	return &Metrics{}, nil
}

func (db *database) Put(ts int64, fields []string, value []byte) (err error) {
	if db.closed.Get() {
		return goerr.Wrap(ErrClosed, 0)
	}

	md := db.metadata
	md.RLock()
	dur := md.Duration()
	res := md.Resolution()
	md.RUnlock()

	// floor ts to a point start time
	ts -= ts % res

	epo, err := db.getEpoch(ts)
	if err != nil {
		return goerr.Wrap(err, 0)
	}

	trmStart := ts - (ts % dur)
	pos := uint32((ts - trmStart) / res)

	err = epo.Put(pos, fields, value)
	if err != nil {
		return goerr.Wrap(err, 0)
	}

	return nil
}

func (db *database) One(start, end int64, fields []string) (out [][]byte, err error) {
	if db.closed.Get() {
		return nil, goerr.Wrap(ErrClosed, 0)
	}

	md := db.metadata
	md.RLock()
	dur := md.Duration()
	res := md.Resolution()
	md.RUnlock()

	// floor ts to a point start time
	start -= start % res
	end -= end % res

	if end <= start {
		return nil, goerr.Wrap(ErrRange, 0)
	}

	epoFirst := start - (start % dur)
	epoLast := end - (end % dur)
	pcount := (end - start) / res

	out = make([][]byte, pcount)

	var trmStart, trmEnd int64

	for ts := epoFirst; ts <= epoLast; ts += dur {
		epo, err := db.getEpoch(ts)
		if err != nil {
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
			continue
		}

		recStart := (trmStart - start) / res
		recEnd := (trmEnd - start) / res
		copy(out[recStart:recEnd], result)
	}

	return out, nil
}

func (db *database) Get(start, end int64, fields []string) (out map[*index.Item][][]byte, err error) {
	if db.closed.Get() {
		return nil, goerr.Wrap(ErrClosed, 0)
	}

	md := db.metadata
	md.RLock()
	dur := md.Duration()
	res := md.Resolution()
	psz := md.PayloadSize()
	md.RUnlock()

	// floor ts to a point start time
	start -= start % res
	end -= end % res

	if end <= start {
		return nil, goerr.Wrap(ErrRange, 0)
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
					set[i] = make([]byte, psz)
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

func (db *database) Sync() (err error) {
	if db.closed.Get() {
		return goerr.Wrap(ErrClosed, 0)
	}

	for _, ep := range db.rwepochs.Data() {
		err = ep.Sync()
		if err != nil {
			return goerr.Wrap(err, 0)
		}
	}

	return nil
}

func (db *database) Close() (err error) {
	if db.closed.Get() {
		db.logger.Error(ErrClosed)
		return nil
	}

	db.epoMutex.Lock()
	defer db.epoMutex.Unlock()

	// Purge will send all epochs to the evict function.
	// The evict function is set inside the New function.
	// epochs will be properly closed there.
	db.roepochs.Purge()
	db.rwepochs.Purge()

	// mark as closed
	db.closed.Set(true)

	db.metadata.Lock()
	defer db.metadata.Unlock()

	if err := db.metadata.Close(); err != nil {
		return goerr.Wrap(err, 0)
	}

	return nil
}

// getEpoch loads a epoch into memory and returns it
// if ro is true, loads the epoch in read-only mode
func (db *database) getEpoch(ts int64) (epo Epoch, err error) {
	if db.closed.Get() {
		return nil, goerr.Wrap(ErrClosed, 0)
	}

	md := db.metadata
	md.RLock()
	dur := md.Duration()
	mrw := md.MaxRWEpochs()
	md.RUnlock()

	// floor ts to a epoch start time
	ts -= ts % dur

	now := vtimer.Now()
	now -= now % dur
	min := now - int64(mrw-1)*dur
	max := now + dur

	if ts >= max {
		return nil, goerr.Wrap(ErrFuture, 0)
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

	var ok bool

	if epo, ok = epochs.Get(ts); ok {
		return epo, nil
	}

	db.epoMutex.Lock()
	defer db.epoMutex.Unlock()

	if epo, ok = epochs.Get(ts); ok {
		return epo, nil
	}

	epo, err = db.loadEpoch(ts, ro)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	epochs.Add(ts, epo)

	return epo, nil
}

func (db *database) loadEpoch(ts int64, ro bool) (epo Epoch, err error) {
	if db.closed.Get() {
		return nil, goerr.Wrap(ErrClosed, 0)
	}

	md := db.metadata
	md.RLock()
	dur := md.Duration()
	res := md.Resolution()
	psz := md.PayloadSize()
	ssz := md.SegmentSize()
	md.RUnlock()

	istr := strconv.Itoa(int(ts))
	tpath := path.Join(db.dbpath, EpochPrefix+istr)

	payloadCount := uint32(dur / res)
	options := &EpochOptions{
		Path:  tpath,
		PSize: psz,
		RSize: payloadCount,
		SSize: ssz,
		ROnly: ro,
	}

	epo, err = NewEpoch(options)
	if err != nil {

		if !goerr.Is(err, ErrNoEpoch) {
			db.logger.Error(err, "failed to load epoch", tpath)
		}

		return nil, goerr.Wrap(err, 0)
	}

	return epo, nil
}

// check for expired epochs every minute until closed
// close expired epochs and delete all expired files
func (db *database) enforceRetention() {
	if db.closed.Get() {
		return
	}

	if num, err := db.expire(); err != nil && err != ErrClosed {
		db.logger.Error(err)
	} else if num > 0 {
		db.logger.Info("expired:", num)
	}

	for _ = range time.Tick(RetInterval) {
		if db.closed.Get() {
			break
		}

		if num, err := db.expire(); err != nil && err != ErrClosed {
			db.logger.Error(err)
			continue
		} else if num > 0 {
			db.logger.Info("expired:", num)
		}
	}
}

func (db *database) expire() (num int, err error) {
	if db.closed.Get() {
		return 0, goerr.Wrap(ErrClosed, 0)
	}

	md := db.metadata
	md.RLock()
	ret := md.Retention()
	dur := md.Duration()
	md.RUnlock()

	ts := vtimer.Now() - ret
	// floor ts to a epoch start time
	ts -= ts % dur

	now := vtimer.Now()
	now -= now % dur

	files, err := ioutil.ReadDir(db.dbpath)

	if os.IsNotExist(err) {
		return 0, nil
	}

	if err != nil {
		return 0, goerr.Wrap(err, 0)
	}

	db.epoMutex.Lock()
	defer db.epoMutex.Unlock()

	for _, finfo := range files {
		fname := finfo.Name()
		if !strings.HasPrefix(fname, EpochPrefix) {
			continue
		}

		tsStr := strings.TrimPrefix(fname, EpochPrefix)
		tsInt, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			db.logger.Error(err)
			continue
		}

		if tsInt > ts {
			continue
		}

		epo, ok := db.roepochs.Del(tsInt)
		if ok {
			err = epo.Close()
			if err != nil {
				db.logger.Error(err)
				continue
			}
		}

		bpath := path.Join(db.dbpath, fname)

		err = os.RemoveAll(bpath)
		if err != nil {
			db.logger.Error(err)
			continue
		}

		num++
	}

	return num, nil
}
