package kdb

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/hashicorp/golang-lru"
	"github.com/meteorhacks/kadiradb-core/index"
	"github.com/meteorhacks/kadiradb-core/term"
	"github.com/meteorhacks/kadiradb-core/utils/logger"
	"github.com/meteorhacks/kdb/clock"
)

const (
	// LoggerPrefix will be used to prefix debug logs
	LoggerPrefix = "KDB"

	// TermDirPrefix will be prefixed to each term directory
	// e.g. term_0, term_10, ... (if term duration is 10)
	TermDirPrefix = "term_"
)

var (
	// ErrDurRes is returned when given duration is not a multiple of resolution
	// Each point in a term represents a `resolution` amount of time (in ns).
	ErrDurRes = errors.New("duration should be a multiple of resolution")
	// ErrFuture is returned when user requests data form a future term
	// It is also returned when user tries to Put data for a future timestamp.
	ErrFuture = errors.New("timestamp is set to a future time")
	// ErrRWTerm is returned when user tries to remove a read-write term
	ErrRWTerm = errors.New("cannot delete read-write terms")
	// ErrRange is returned when thegiven range is not valid
	ErrRange = errors.New("provided time range is not valid")
)

// Database is a time series database which can store fixed sized payloads.
// Data can be queried using dynamic number of fields with specific value
// or wildcard values (only supports "" for match-all at the moment).
type Database interface {
	// Put stores data in the database for specific timestamp and set of fields
	Put(ts int64, fields []string, value []byte) (err error)

	// Get gets a series of data points from the database
	// Data can be taken from one or more `terms`.
	Get(start, end int64, fields []string) (out map[*index.Item][][]byte, err error)

	// Expire removes all terms before given timestamp
	Expire(ts int64) (err error)

	// Close cleans up stuff, releases resources and closes the database.
	Close() (err error)
}

// Options has parameters required for creating a `Database`
type Options struct {
	BasePath      string // directory to store terms
	Resolution    int64  // resolution as a string
	TermDuration  int64  // duration of a single term
	PayloadSize   int64  // size of payload (point) in bytes
	PayloadCount  int64  // number of payloads in a record
	SegmentLength int64  // number of records in a segment
	MaxROTerms    int64  // maximum read-only buckets (uses file handlers)
	MaxRWTerms    int64  // maximum read-write buckets (uses memory maps)
}

type database struct {
	opts    *Options    // options
	roterms *lru.Cache  // a cache to hold read-only terms
	rwterms *lru.Cache  // a cache to hold read-write terms
	trmMutx *sync.Mutex // mutex to control opening closing terms
}

// New creates an new `Database` with given `Options`
// If a database does not exist, it will be created.
func New(options *Options) (_db Database, err error) {
	if options.TermDuration%options.Resolution != 0 {
		logger.Log(LoggerPrefix, ErrDurRes)
		return nil, ErrDurRes
	}

	// evictFn is called when the lru cache runs out of space
	evictFn := func(k interface{}, v interface{}) {
		trm := v.(term.Term)
		err := trm.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
		}
	}

	roterms, err := lru.NewWithEvict(int(options.MaxROTerms), evictFn)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	rwterms, err := lru.NewWithEvict(int(options.MaxRWTerms), evictFn)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	db := &database{
		opts:    options,
		roterms: roterms,
		rwterms: rwterms,
		trmMutx: &sync.Mutex{},
	}

	return db, nil
}

func (db *database) Put(ts int64, fields []string, value []byte) (err error) {
	// floor ts to a point start time
	ts -= ts % db.opts.Resolution

	trm, err := db.getTerm(ts)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	trmStart := ts - (ts % db.opts.TermDuration)
	pos := (ts - trmStart) / db.opts.Resolution

	err = trm.Put(pos, fields, value)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}

func (db *database) Get(start, end int64, fields []string) (out map[*index.Item][][]byte, err error) {
	// floor ts to a point start time
	start -= start % db.opts.Resolution
	end -= end % db.opts.Resolution

	if end >= start {
		return nil, ErrRange
	}

	trmFirst := start - (start % db.opts.TermDuration)
	trmLast := end - (end % db.opts.TermDuration)
	pointCount := (end - start) / db.opts.Resolution

	tmpPoints := make(map[string][][]byte)
	tmpFields := make(map[string][]string)

	var trmStart, trmEnd int64

	for ts := trmFirst; ts <= trmLast; ts += db.opts.TermDuration {
		trm, err := db.getTerm(ts)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			continue
		}

		// if it's the first bucket
		// skip payloads before `start` time
		// defaults to base time of the bucket
		if ts == trmFirst {
			trmStart = start
		} else {
			trmStart = ts
		}

		// if this is the last bucket
		// skip payloads after `end` time
		// defaults to end of the bucket
		if ts == trmEnd {
			trmEnd = end
		} else {
			trmEnd = ts + db.opts.TermDuration
		}

		res, err := trm.Get(trmStart, trmEnd, fields)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			continue
		}

		for item, points := range res {
			key := strings.Join(item.Fields, `¯\\_(ツ)_/¯`)
			set, ok := tmpPoints[key]
			if !ok {
				set = make([][]byte, pointCount, pointCount)

				var i int64
				for i = 0; i < pointCount; i++ {
					set[i] = make([]byte, db.opts.PayloadSize)
				}

				tmpPoints[key] = set
				tmpFields[key] = item.Fields
			}

			recStart := (trmStart - start) / db.opts.Resolution
			recEnd := (trmEnd - end) / db.opts.Resolution
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
	// floor ts to a term start time
	ts -= ts % db.opts.TermDuration

	now := clock.Now()
	now -= now % db.opts.TermDuration
	min := now - (db.opts.MaxRWTerms-1)*db.opts.TermDuration

	if ts >= min {
		return ErrRWTerm
	}

	files, err := ioutil.ReadDir(db.opts.BasePath)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	for _, finfo := range files {
		fname := finfo.Name()
		if !strings.HasPrefix(fname, TermDirPrefix) {
			continue
		}

		tsStr := strings.TrimPrefix(fname, TermDirPrefix)
		tsInt, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			continue
		}

		if tsInt >= ts {
			continue
		}

		v, ok := db.roterms.Peek(tsInt)
		if ok {
			trm := v.(term.Term)
			err = trm.Close()
			if err != nil {
				logger.Log(LoggerPrefix, err)
				continue
			}
		}

		bpath := path.Join(db.opts.BasePath, fname)
		err = os.RemoveAll(bpath)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			continue
		}
	}

	return nil
}

func (db *database) Close() (err error) {
	err = db.closeCachedTerms(db.roterms)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	err = db.closeCachedTerms(db.rwterms)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}

func (db *database) closeCachedTerms(terms *lru.Cache) (err error) {
	keys := terms.Keys()
	for _, k := range keys {
		v, ok := terms.Get(k)
		if !ok {
			continue
		}

		trm := v.(term.Term)
		err = trm.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}
	}

	terms.Purge()

	return nil
}

// getTerm loads a term into memory and returns it
// if ro is true, loads the term in read-only mode
func (db *database) getTerm(ts int64) (trm term.Term, err error) {
	// floor ts to a term start time
	ts -= ts % db.opts.TermDuration

	now := clock.Now()
	now -= now % db.opts.TermDuration
	min := now - (db.opts.MaxRWTerms-1)*db.opts.TermDuration
	max := now + db.opts.TermDuration

	if ts >= max {
		logger.Log(LoggerPrefix, ErrFuture)
		return nil, ErrFuture
	}

	// decide whether we need a read-only or read-write term
	// present term is also included when calculating `min`
	ro := ts >= min

	var terms *lru.Cache
	if ro {
		terms = db.roterms
	} else {
		terms = db.rwterms
	}

	val, ok := terms.Get(ts)
	if ok {
		trm = val.(term.Term)
		return trm, nil
	}

	istr := strconv.Itoa(int(ts))
	tpath := path.Join(db.opts.BasePath, TermDirPrefix+istr)
	options := &term.Options{
		Path:          tpath,
		PayloadSize:   db.opts.PayloadSize,
		PayloadCount:  db.opts.PayloadCount,
		SegmentLength: db.opts.SegmentLength,
		ReadOnly:      ro,
	}

	if ro {
		trm, err = term.Open(options)
	} else {
		trm, err = term.New(options)
	}

	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	terms.Add(ts, trm)

	return trm, nil
}
