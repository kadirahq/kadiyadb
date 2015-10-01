package index

import "sync/atomic"

// Index stores record IDs for each unique field combination.
type Index struct {
	root *TNode
	logs *Logs
	snap *Snap
}

// NewRO loads an existing index in read-only mode
func NewRO(dir string) (i *Index, err error) {
	for {
		// NOTE: Not a loop. Using this to BREAK.
		snap, err := NewSnap(dir)
		if err != nil {
			break
		}

		root, err := snap.LoadRoot()
		if err != nil {
			snap.Close()
			break
		}

		i = &Index{
			root: root,
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

	snap, err := NewSnap(dir)
	if err != nil {
		return nil, err
	}

	if err := snap.Store(root); err != nil {
		snap.Close()
		return nil, err
	}

	i = &Index{
		root: root,
		snap: snap,
	}

	return i, nil
}

// NewRW loads an existing index in read-write mode
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
		tn.Node.RecordID = atomic.AddUint64(&i.logs.nextID, 1) - 1
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

// Close closes the index
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

func (i *Index) ensureBranch(fields []string) (err error) {
	if i.snap != nil {
		// TODO ensure branch is loaded
	}

	return nil
}
