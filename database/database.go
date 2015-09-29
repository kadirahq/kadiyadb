package database

import "github.com/kadirahq/kadiyadb/block"

// DB is a database
type DB struct {
	// TODO add fields
}

// Params is used when creating a new database
type Params struct {
	// TODO add fields
}

// Info has db info
type Info struct {
	// TODO add fields
}

// New creates a new database with given params
func New(p Params) (d *DB, err error) {
	return nil, nil
}

// Track records a measurement
func (d *DB) Track(pid int64, fields []string, total float64, count uint64) (err error) {
	return nil
}

// Fetch fetches data from database
func (d *DB) Fetch(from, to int64, fields []string) (res []block.Point, err error) {
	res = block.Empty[:0]
	return res, nil
}

// Sync flushes pending writes to the filesystem
func (d *DB) Sync() (err error) {
	return nil
}

// Info returns database information
func (d *DB) Info() (info *Info, err error) {
	return nil, nil
}
