package index

import (
	"path"

	"github.com/kadirahq/go-tools/segmmap"
)

const (
	prefixsnap = "snap_"
	segszsnap  = 1024 * 1024 * 20
)

type offset struct {
	from int64
	to   int64
}

// Snap helps create and load index trees from snapshot files.
// Index snapshots are read-only. TNodees are loaded when needed.
type Snap struct {
	dataFile *segmmap.Map
	offsets  map[string]offset
}

// NewSnap creates a log type index persister.
func NewSnap(dir string) (s *Snap, err error) {
	segpath := path.Join(dir, prefixsnap)
	rf, err := segmmap.NewMap(segpath, segszsnap)
	if err != nil {
		return nil, err
	}

	if err := rf.LoadAll(); err != nil {
		return nil, err
	}

	df, err := segmmap.NewMap(segpath, segszsnap)
	if err != nil {
		return nil, err
	}

	if err := df.LoadAll(); err != nil {
		return nil, err
	}

	if err := rf.Close(); err != nil {
		return nil, err
	}

	s = &Snap{
		dataFile: df,
		offsets:  map[string]offset{},
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

// Sync syncs the snapshot store
func (s *Snap) Sync() (err error) {
	if err := s.dataFile.Sync(); err != nil {
		return err
	}

	return nil
}

// Close closes the snapshot store
func (s *Snap) Close() (err error) {
	if err := s.dataFile.Close(); err != nil {
		return err
	}

	return nil
}
