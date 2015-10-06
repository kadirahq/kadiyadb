package database

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"path"

	"github.com/kadirahq/kadiyadb/epoch"
)

const (
	paramfile = "params.json"
)

var (
	// ErrInvTime is returned when the timestamp is invalid
	ErrInvTime = errors.New("invalid timestamp")
)

// Handler is a function which is called with a result
type Handler func(result []*Chunk, err error)

// DB is a database
type DB struct {
	params *Params
	cache  *epoch.Cache
	rsize  int64
}

// Params is used when creating a new database
type Params struct {
	Duration    int64 `json:"duration"`
	Retention   int64 `json:"retention"`
	Resolution  int64 `json:"resolution"`
	MaxROEpochs int64 `json:"maxROEpochs"`
	MaxRWEpochs int64 `json:"maxRWEpochs"`
}

// LoadAll loads all databases inside the path
func LoadAll(dir string) (dbs map[string]*DB) {
	dbs = map[string]*DB{}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil
	}

	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		name := file.Name()
		base := path.Join(dir, name)
		file := path.Join(base, paramfile)
		data, err := ioutil.ReadFile(file)
		if err != nil {
			continue
		}

		params := &Params{}
		if err := json.Unmarshal(data, params); err != nil {
			continue
		}

		db, err := Open(base, params)
		if err != nil {
			continue
		}

		dbs[name] = db
	}

	return dbs
}

// Open opens an existing database with given parameters
func Open(dir string, p *Params) (db *DB, err error) {
	// TODO validate all parameters
	rsize := p.Duration / p.Resolution

	db = &DB{
		params: p,
		cache:  epoch.NewCache(p.MaxRWEpochs, p.MaxROEpochs, dir, rsize),
		rsize:  rsize,
	}

	return db, nil
}

// Track records a measurement
func (d *DB) Track(ts uint64, fields []string, total float64, count uint64) (err error) {
	ets, pos := d.split(ts)

	if ets < 0 {
		return ErrInvTime
	}

	e, err := d.cache.LoadRW(ets)
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

	ets0, pos0 := d.split(from)
	ets1, pos1 := d.split(to)

	// no points to fetch on last epoch
	// decrease final epoch timestamp
	if pos1 == 0 {
		ets1 -= d.params.Duration
		pos1 = d.rsize
	}

	// check timestamp bounds
	if ets0 < 0 || ets1 < 0 {
		fn(nil, ErrInvTime)
		return
	}

	// no points in given time range
	if ets0 == ets1 && pos0 == pos1 {
		fn([]*Chunk{}, nil)
		return
	}

	nchunks := (ets1-ets0)/d.params.Duration + 1
	chunks := make([]*Chunk, 0, nchunks)

	for ets := ets0; ets <= ets1; ets += d.params.Duration {
		var start int64
		end := d.params.Duration

		if ets == ets0 {
			start = pos0
		}

		if ets == ets1 {
			end = pos1
		}

		e, err := d.cache.LoadRO(ets)
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
			From:   uint64(ets0 + start*d.params.Resolution),
			To:     uint64(ets1 + end*d.params.Resolution),
			Series: series,
		}

		chunks = append(chunks, chunk)
	}

	fn(chunks, nil)
	return
}

// Sync flushes pending writes to the filesystem
func (d *DB) Sync() (err error) {
	if err := d.cache.Sync(); err != nil {
		return err
	}

	return nil
}

// split the time into epoch start time and point position
func (d *DB) split(ts uint64) (ets, pos int64) {
	t64 := int64(ts)
	ets = t64 - t64%d.params.Duration
	pos = (t64 - ets) / d.params.Resolution
	return ets, pos
}
