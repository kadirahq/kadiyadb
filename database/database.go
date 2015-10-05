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
type Handler func(result []*Chunk, err error)

// DB is a database
type DB struct {
	Info *Info
	roc  *epoch.Cache
	rwc  *epoch.Cache
	rsz  int64 // d.Info.Duration / d.Info.Resolution
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
	// TODO code!
	return map[string]*DB{}
}

// New creates a new database with given params set
// Server must be restarted for this to take effect
func New(dir string, p *Params) (d *DB, err error) {
	// TODO code!
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
	if to < from {
		fn(nil, ErrInvTime)
		return
	}

	ets0, pos0 := d.breakdown(from)
	ets1, pos1 := d.breakdown(to)
	max := time.Now().UnixNano()
	max -= max % d.Info.Duration
	min := max - d.Info.Retention

	// no points to fetch on last epoch
	// decrease final epoch timestamp
	if pos1 == 0 {
		ets1 -= d.Info.Duration
		pos1 = d.rsz
	}

	// check timestamp bounds against retention and current time
	if ets0 > max || ets0 < min || ets1 > max || ets1 < min {
		fn(nil, ErrInvTime)
		return
	}

	// no points in given time range
	if ets0 == ets1 && pos0 == pos1 {
		fn([]*Chunk{}, nil)
		return
	}

	nchunks := (ets1-ets0)/d.Info.Duration + 1
	chunks := make([]*Chunk, 0, nchunks)

	for ets := ets0; ets <= ets1; ets += d.Info.Duration {
		var start int64
		end := d.Info.Duration

		if ets == ets0 {
			start = pos0
		}

		if ets == ets1 {
			end = pos1
		}

		e, err := d.Epoch(ets0, true)
		if err != nil {
			fn(nil, err)
			return
		}

		// epochs are RLocked to make sure they are not closed while in use
		// memory locations of Points are valid only when epochs are available
		// epoch read locks are unlocked after running the handler function
		e.RLock()
		defer e.RUnlock()

		points, nodes, err := e.Fetch(start, end, fields)
		if err != nil {
			fn(nil, err)
			return
		}

		count := len(points)
		series := make([]*Series, count)

		for i := 0; i < count; i++ {
			series[i] = &Series{
				Fields: nodes[i].Fields,
				Points: points[i],
			}
		}

		chunk := &Chunk{
			From:   uint64(ets0 + start*d.Info.Resolution),
			To:     uint64(ets1 + end*d.Info.Resolution),
			Series: series,
		}

		chunks = append(chunks, chunk)
	}

	fn(chunks, nil)
	return
}

// Sync flushes pending writes to the filesystem
func (d *DB) Sync() (err error) {
	// TODO code!
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
