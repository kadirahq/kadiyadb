package epoch

import (
	"sync"

	"github.com/kadirahq/kadiyadb/block"
	"github.com/kadirahq/kadiyadb/index"
)

// Epoch ...
type Epoch struct {
	*sync.RWMutex

	index *index.Index
	block *block.Block
}

// NewRW ...
func NewRW(dir string, rsz int64) (e *Epoch, err error) {
	b, err := block.New(dir, rsz)
	if err != nil {
		return nil, err
	}

	if err := b.Lock(); err != nil {
		return nil, err
	}

	i, err := index.NewRW(dir)
	if err != nil {
		return nil, err
	}

	e = &Epoch{
		block:   b,
		index:   i,
		RWMutex: &sync.RWMutex{},
	}

	return e, nil
}

// NewRO ...
func NewRO(dir string, rsz int64) (e *Epoch, err error) {
	b, err := block.New(dir, rsz)
	if err != nil {
		return nil, err
	}

	i, err := index.NewRW(dir)
	if err != nil {
		return nil, err
	}

	e = &Epoch{
		block:   b,
		index:   i,
		RWMutex: &sync.RWMutex{},
	}

	return e, nil
}

// Track records a measurement
func (e *Epoch) Track(pid int64, fields []string, total float64, count uint64) (err error) {
	for i, l := 1, len(fields); i <= l; i++ {
		fieldset := fields[:i]
		node, err := e.index.Ensure(fieldset)
		if err != nil {
			return err
		}

		if err := e.block.Track(node.RecordID, pid, total, count); err != nil {
			return err
		}
	}

	return nil
}

// Fetch fetches data from database
func (e *Epoch) Fetch(from, to int64, fields []string) (points [][]block.Point, nodes []*index.Node, err error) {
	nodes, err = e.index.Find(fields)
	if err != nil {
		return nil, nil, err
	}

	points = make([][]block.Point, len(nodes))
	for i, node := range nodes {
		points[i], err = e.block.Fetch(node.RecordID, from, to)
		if err != nil {
			return nil, nil, err
		}
	}

	return points, nodes, nil
}

// Sync flushes pending writes to the filesystem
func (e *Epoch) Sync() (err error) {
	if err := e.block.Sync(); err != nil {
		return err
	}
	if err := e.index.Sync(); err != nil {
		return err
	}

	return nil
}

// Close closes the epoch and frees used resources
func (e *Epoch) Close() (err error) {
	if err := e.block.Close(); err != nil {
		return err
	}
	if err := e.index.Close(); err != nil {
		return err
	}

	return nil
}
