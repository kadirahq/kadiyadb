package index

import (
	"path"

	"github.com/kadirahq/go-tools/segmmap"
)

const (
	prefixsnap = "snap_"
)

// Snap helps create and load index trees from snapshot files.
// Index snapshots are read-only. TNodees are loaded when needed.
type Snap struct {
	rootFile *segmmap.Map
	dataFile *segmmap.Map
	offsets  map[string]struct{ from, to int64 }
}

// NewSnap creates a log type index persister.
func NewSnap(dir string) (s *Snap, err error) {
	segpath := path.Join(dir, prefixsnap)
	rf, err := segmmap.NewMap(segpath, segsz)
	if err != nil {
		return nil, err
	}

	if err := rf.LoadAll(); err != nil {
		return nil, err
	}

	df, err := segmmap.NewMap(segpath, segsz)
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

// Store ...
func (s *Snap) Store(tree *TNode) (err error) {
	return nil
}

// LoadRoot ...
func (s *Snap) LoadRoot() (tree *TNode, err error) {
	return nil, nil
}

// LoadBranch ...
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
