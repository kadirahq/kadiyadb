package database

import (
	"errors"
	"time"

	"github.com/kadirahq/kadiyadb/epoch"
)

var (
	// ErrInvTime is returned when the timestamp is invalid
	ErrInvTime = errors.New("invalid timestamp")
)

// Handler is a function which is called with a result
type Handler func(result []*Series, err error)

// DB is a database
type DB struct {
	Info *Info
	roc  *epoch.Cache
	rwc  *epoch.Cache
}

// Params is used when creating a new database
type Params struct {
	Duration    int64
	Retention   int64
	Resolution  int64
	MaxROEpochs int64
	MaxRWEpochs int64
}

// Info has db info
type Info struct {
	Duration   int64
	Retention  int64
	Resolution int64
}

// LoadDatabases loads all databases inside the path
func LoadDatabases(dir string) (dbs map[string]*DB) {
	return map[string]*DB{}
}

// New creates a new database with given params set
// Server must be restarted for this to take effect
func New(dir string, p *Params) (d *DB, err error) {
	// TODO: write config file to directory
	return nil, nil
}

// Track records a measurement
func (d *DB) Track(ts uint64, fields []string, total float64, count uint64) (err error) {
	ets, pos := d.breakdown(ts)
	max := time.Now().UnixNano()
	max -= max % d.Info.Duration
	min := max - d.Info.Retention

	if ets > max || ets < min {
		// TODO use shared error value instead
		return errors.New("ts out of bounds")
	}

	e, err := d.Epoch(ets, true)
	if err != nil {
		return err
	}

	err = e.Track(pos, fields, total, count)
	if err != nil {
		return err
	}

	return nil
}

// Fetch fetches data from database
func (d *DB) Fetch(from, to uint64, fields []string, fn Handler) {
	ets0, pos0 := d.breakdown(from)
	ets1, pos1 := d.breakdown(to)
	max := time.Now().UnixNano()
	max -= max % d.Info.Duration
	min := max - d.Info.Retention

	if ets0 > max || ets0 < min || ets1 > max || ets1 < min || to < from {
		fn(nil, ErrInvTime)
		return
	}

	// // fast path!
	// if ets0 == ets1 {
	// 	e, err := d.Epoch(ets0, true)
	// 	if err != nil {
	// 		fn(nil, nil, err)
	// 		return
	// 	}
	//
	// 	e.RLock()
	// 	defer e.RUnlock()
	//
	// 	points, nodes, err := e.Fetch(pos0, pos1, fields)
	// 	fn(points, nodes, err)
	// }

	_ = pos0
	_ = pos1
}

// Sync flushes pending writes to the filesystem
func (d *DB) Sync() (err error) {
	return nil
}

// Epoch returns the epoch for given start timestamp
func (d *DB) Epoch(ts int64, write bool) (e *epoch.Epoch, err error) {
	// TODO code!
	return nil, nil
}

func (d *DB) breakdown(ts uint64) (ets, pos int64) {
	t64 := int64(ts)
	ets = t64 - t64%d.Info.Duration
	pos = (t64 - ets) / d.Info.Resolution
	return ets, pos
}
