package epoch

import (
	"sync"

	"github.com/kadirahq/kadiyadb-protocol"
	"github.com/kadirahq/kadiyadb/block"
	"github.com/kadirahq/kadiyadb/index"
)

// Epoch is a partition of database data created by measurement timestamps.
// Each epoch has it's own index tree and block data store. Changes made to
// one epoch will not affect any values of other epochs.
type Epoch struct {
	*sync.RWMutex

	index *index.Index
	block block.Block
}

// NewRW function will load an epoch in read-write mode
func NewRW(dir string, rsz int64) (e *Epoch, err error) {
	b, err := block.NewRW(dir, rsz)
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

// NewRO function will load an epoch in read-only mode
func NewRO(dir string, rsz int64) (e *Epoch, err error) {
	b, err := block.NewRO(dir, rsz)
	if err != nil {
		return nil, err
	}

	i, err := index.NewRO(dir)
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

// Track records a measurement with given total value and measurement count
// The record is identified by an array of string fields which will be used
// in the index. The position of the point in the record is given as `pid`.
func (e *Epoch) Track(pid int64, fields []string, total, count float64) (err error) {
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

// Fetch fetches data from database from zero or more matching records
// Matching records are identified from the index by given array of fields.
// For each matching recods, points within the given range are extracted.
// Finally the function returns both index nodes and points separately.
func (e *Epoch) Fetch(from, to int64, fields []string) (points [][]protocol.Point, nodes []*index.Node, err error) {
	nodes, err = e.index.Find(fields)
	if err != nil {
		return nil, nil, err
	}

	points = make([][]protocol.Point, len(nodes))
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

// Close releases resources
func (e *Epoch) Close() (err error) {
	e.Lock()
	defer e.Unlock()

	if err := e.block.Close(); err != nil {
		return err
	}
	if err := e.index.Close(); err != nil {
		return err
	}

	return nil
}
