package kadiyadb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"path"
	"time"

	"github.com/kadirahq/kadiyadb-protocol"
	"github.com/kadirahq/kadiyadb/epoch"
)

const (
	// paramfile is the name of the config file placed in the database directory.
	// Param files are only read when the database server starts therefore
	// a server re-start is required for changes to take effect (for now).
	//
	// Param File Format:
	//
	//   {
	//     "duration": "1h",
	//     "resolution": "1m",
	//     "retention": "24h",
	//     "maxROEpochs": 12,
	//     "maxRWEpochs": 2
	//   }
	//
	paramfile = "params.json"
)

var (
	// ErrInvParams is returned when the db params are invalid
	ErrInvParams = errors.New("invalid database parameters")

	// ErrInvTime is returned when the timestamp is invalid
	ErrInvTime = errors.New("invalid timestamp")
)

// Handler is a function which is called with Fetch result
// The data returned here is only valid inside this function
// For extended use of results, a copy of the data must be made.
type Handler func(result []*protocol.Chunk, err error)

// Params is used when creating a new database
type Params struct {
	DurationStr   string `json:"duration"`
	Duration      int64  `json:"-"`
	ResolutionStr string `json:"resolution"`
	Resolution    int64  `json:"-"`
	RetentionStr  string `json:"retention"`
	Retention     int64  `json:"-"`
	MaxROEpochs   int64  `json:"maxROEpochs"`
	MaxRWEpochs   int64  `json:"maxRWEpochs"`
}

// DB is a database
type DB struct {
	params *Params
	cache  *epoch.Cache
	rsize  int64
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
			fmt.Println("DB Error: params:", name, err)
			continue
		}

		if dur, err := time.ParseDuration(params.DurationStr); err != nil {
			fmt.Println("DB Error: duration", name, params.DurationStr, err)
			continue
		} else {
			params.Duration = int64(dur)
		}

		if dur, err := time.ParseDuration(params.ResolutionStr); err != nil {
			fmt.Println("DB Error: resolution", name, params.ResolutionStr, err)
			continue
		} else {
			params.Resolution = int64(dur)
		}

		if dur, err := time.ParseDuration(params.RetentionStr); err != nil {
			fmt.Println("DB Error: retention", name, params.RetentionStr, err)
			continue
		} else {
			params.Retention = int64(dur)
		}

		db, err := Open(base, params)
		if err != nil {
			fmt.Println("DB Error: open:", name, err)
			continue
		}

		dbs[name] = db
	}

	return dbs
}

// Open opens an existing database with given parameters
func Open(dir string, p *Params) (db *DB, err error) {
	if p == nil ||
		p.Duration == 0 ||
		p.Resolution == 0 ||
		p.Retention == 0 ||
		p.MaxROEpochs == 0 ||
		p.MaxRWEpochs == 0 ||
		p.Duration%p.Resolution != 0 ||
		p.Retention%p.Duration != 0 {
		return nil, ErrInvParams
	}

	rsize := p.Duration / p.Resolution
	cache := epoch.NewCache(p.MaxRWEpochs, p.MaxROEpochs, dir, rsize)

	db = &DB{
		params: p,
		cache:  cache,
		rsize:  rsize,
	}

	return db, nil
}

// Track records a measurement with given total value and measurement count.
// It uses the field combination and the timestamp to locate the data point.
func (d *DB) Track(ts uint64, fields []string, total, count float64) (err error) {
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

// Fetch fetches data from database by given field pattern and timestamp range.
// The handler function is called with the result and errors (if any).
func (d *DB) Fetch(from, to uint64, fields []string, fn Handler) {
	if to < from {
		fn(nil, ErrInvTime)
		return
	}

	ets0, pos0 := d.split(from)
	ets1, pos1 := d.split(to)

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
		fn([]*protocol.Chunk{}, nil)
		return
	}

	nchunks := (ets1-ets0)/d.params.Duration + 1
	chunks := make([]*protocol.Chunk, 0, nchunks)

	for ets := ets0; ets <= ets1; ets += d.params.Duration {
		var start int64
		end := d.rsize

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
		series := make([]*protocol.Series, count)

		for i := 0; i < count; i++ {
			series[i] = &protocol.Series{
				Fields: nodes[i].Fields,
				Points: points[i],
			}
		}

		chunk := &protocol.Chunk{
			From:   uint64(ets + start*d.params.Resolution),
			To:     uint64(ets + end*d.params.Resolution),
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
	if t64 < d.params.Resolution {
		return 0, 0
	}

	ets = d.params.Duration * (t64 / d.params.Duration)
	pos = (t64 - ets) / d.params.Resolution

	return ets, pos
}
