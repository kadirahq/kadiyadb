package index

import (
	"errors"
	"sync/atomic"
)

var (
	// ErrInvFields is given when requested fields are invalid
	ErrInvFields = errors.New("requested fields are not valid")
)

// Index stores record IDs for each unique field combination as a tree.
// The index tree starts from a single root node and can have many levels.
// Index tree may use an append only log or a snapshot to read/write to disk.
type Index struct {
	root *TNode
	logs *Logs
	snap *Snap
}

// NewRO loads an existing index in read-only mode. It will attempt to load
// it from a snapshot file first and if it fails, it'll fallback to using the
// append log. A new snapshot will be created before returning this function.
// Branches of the read only index are loaded only when it's required.
func NewRO(dir string) (i *Index, err error) {
	snap, err := LoadSnap(dir)
	if snap, err := LoadSnap(dir); err == nil {
		i = &Index{
			root: snap.RootNode,
			snap: snap,
		}

		return i, nil
	}

	// If we've come to this point, snapshot data doesn't exist or is corrupt
	// Try to load data from log files if available and immediately create a
	// new snapshot which can be used when this index is loaded next time.

	logs, err := NewLogs(dir)
	if err != nil {
		return nil, err
	}

	root, err := logs.Load()
	if err != nil {
		return nil, err
	}

	if err := logs.Close(); err != nil {
		return nil, err
	}

	if snap, err = writeSnapshot(dir, root); err != nil {
		// TODO handle snapshot store error
	}

	i = &Index{
		root: root,
		snap: snap,
	}

	return i, nil
}

// NewRW loads an existing index in read-write mode. This will always use the
// append log to write data. This index will always have all index nodes ready.
func NewRW(dir string) (i *Index, err error) {
	logs, err := NewLogs(dir)
	if err != nil {
		return nil, err
	}

	root, err := logs.Load()
	if err != nil {
		return nil, err
	}

	i = &Index{
		root: root,
		logs: logs,
	}

	return i, nil
}

// Ensure inserts a new node to the index if it's not available.
func (i *Index) Ensure(fields []string) (node *Node, err error) {
	tn := i.root.Ensure(fields)

	tn.Mutex.Lock()
	if tn.Node.RecordID == Placeholder {
		tn.Node.RecordID = atomic.AddInt64(&i.logs.nextID, 1) - 1
		if err := i.logs.Store(tn); err != nil {
			tn.Mutex.Unlock()
			return nil, err
		}
	}
	tn.Mutex.Unlock()

	return tn.Node, nil
}

// Find finds all existing index nodes with given field pattern.
// The '*' can be used to match any value for the index field.
func (i *Index) Find(fields []string) (ns []*Node, err error) {
	if err := i.ensureBranch(fields); err != nil {
		return nil, err
	}

	return i.root.Find(fields)
}

// FindOne finds the index nodes with exact given field combination.
// `n` is nil if the no nodes exist in the index with given fields.
func (i *Index) FindOne(fields []string) (n *Node, err error) {
	if err := i.ensureBranch(fields); err != nil {
		return nil, err
	}

	return i.root.FindOne(fields)
}

// Sync syncs the index
func (i *Index) Sync() (err error) {
	if i.logs != nil {
		if err := i.logs.Sync(); err != nil {
			return err
		}
	}

	if i.snap != nil {
		if err := i.snap.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// Close releases resources
func (i *Index) Close() (err error) {
	if i.logs != nil {
		if err := i.logs.Close(); err != nil {
			return err
		}
	}

	if i.snap != nil {
		if err := i.snap.Close(); err != nil {
			return err
		}
	}

	return nil
}

// ensureBranch makes sure that the branch starting from the first level
// of the index tree is loaded from the snapshot data file. The root file
// contains all nodes from the first level of the tree and their offsets
// These nodes will have "nil" value in roots Children map which will be
// used to identify branches not yet loaded from the snapshot data file.
func (i *Index) ensureBranch(fields []string) (err error) {
	if i.snap == nil {
		return nil
	}

	if len(fields) == 0 {
		return ErrInvFields
	}

	// snapshot only supports a single level for now
	// perhaps this can be made configurable later.
	name := fields[0]

	// faster path!
	// missing/ready
	i.root.Mutex.RLock()
	if br, ok := i.root.Children[name]; !ok {
		// item not in index
		i.root.Mutex.RUnlock()
		return nil
	} else if br != nil {
		// item already loaded
		i.root.Mutex.RUnlock()
		return nil
	}
	i.root.Mutex.RUnlock()

	// slower path!
	// should load
	i.root.Mutex.Lock()
	defer i.root.Mutex.Unlock()

	// test it again to avoid multiple loads
	if br, ok := i.root.Children[name]; !ok {
		return nil
	} else if br != nil {
		return nil
	}

	br, err := i.snap.Branch(name)
	if err != nil {
		return err
	}

	i.root.Children[name] = br

	return nil
}
