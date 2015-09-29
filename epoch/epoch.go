package epoch

import "github.com/kadirahq/kadiyadb/block"

// Epoch ...
type Epoch struct {
	// TODO add fields
}

// NewRW ...
func NewRW(dir string, rsz int64) (e *Epoch, err error) {
	return nil, nil
}

// NewRO ...
func NewRO(dir string, rsz int64) (e *Epoch, err error) {
	return nil, nil
}

// Track records a measurement
func (e *Epoch) Track(pid int64, fields []string, total float64, count uint64) (err error) {
	return nil
}

// Fetch fetches data from database
func (e *Epoch) Fetch(from, to int64, fields []string) (res []block.Point, err error) {
	res = block.Empty[:0]
	return res, nil
}

// Sync flushes pending writes to the filesystem
func (e *Epoch) Sync() (err error) {
	return nil
}

// Close closes the epoch and frees used resources
func (e *Epoch) Close() (err error) {
	return nil
}
