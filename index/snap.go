package index

import "github.com/kadirahq/go-tools/segmmap"

// Snap helps create and load index trees from snapshot files.
// Index snapshots are read-only. TNodees are loaded when needed.
type Snap struct {
	rootFile *segmmap.Map
	dataFile *segmmap.Map
	offsets  map[string]struct{ from, to int64 }
}

// NewSnap creates a log type index persister.
func NewSnap(path string) (s *Snap, err error) {
	rf, err := segmmap.NewMap(path, segsz)
	if err != nil {
		return nil, err
	}

	if err := rf.LoadAll(); err != nil {
		return nil, err
	}

	df, err := segmmap.NewMap(path, segsz)
	if err != nil {
		return nil, err
	}

	if err := df.LoadAll(); err != nil {
		return nil, err
	}

	s = &Snap{
		rootFile: rf,
		dataFile: df,
		offsets:  map[string]struct{ from, to int64 }{},
	}

	return s, nil
}

func (s *Snap) Store(tree *TNode) (err error) {
	return nil
}

func (s *Snap) LoadRoot() (tree *TNode, err error) {
	return nil, nil
}

func (s *Snap) LoadBranch() (tree *TNode, err error) {
	return nil, nil
}

// Close closes the snapshot store
func (s *Snap) Close() (err error) {
	if err := s.rootFile.Close(); err != nil {
		return err
	}
	if err := s.dataFile.Close(); err != nil {
		return err
	}

	return nil
}
