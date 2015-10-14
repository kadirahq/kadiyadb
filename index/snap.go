package index

import (
	"errors"
	"path"

	"github.com/kadirahq/go-tools/segments"
	"github.com/kadirahq/go-tools/segments/segfile"
)

const (
	// index file prefix when stored in snapshot format
	// index files will be named "snap_0, snap_1, ..."
	prefixsnap = "snap_"

	// Size of the segment file
	// !IMPORTANT if this value changes, the database will not be able to use
	// older data. To avoid accidental changes, this value is hardcoded here.
	segszsnap = 1024 * 1024 * 20
)

// Snap helps create and load index pre-built index trees from snapshot files.
// Index snapshots are read-only, any changes require a rebuild of the snapshot.
type Snap struct {
	RootNode *TNode
	dataFile segments.Store
	branches *SnapInfo
}

// LoadSnap opens an index persister which stores pre-built index trees.
// When loading a index snapshot, only the top level of the tree is loaded.
// All other tree branches are loaded only when it's necessary (on request).
func LoadSnap(dir string) (s *Snap, err error) {
	segpath := path.Join(dir, prefixsnap)
	rf, err := segfile.New(segpath, segszsnap)
	if err != nil {
		return nil, err
	}

	// TODO init info and root
	var info *SnapInfo
	var root *TNode

	if err := rf.Close(); err != nil {
		return nil, err
	}

	df, err := segfile.New(segpath, segszsnap)
	if err != nil {
		return nil, err
	}

	s = &Snap{
		RootNode: root,
		dataFile: df,
		branches: info,
	}

	return s, errors.New("")
}

// Branch function loads a branch from the data memory map
func (s *Snap) Branch(key string) (tree *TNode, err error) {
	// ! TODO load tree branch from a snapshot
	return nil, nil
}

// Sync syncs the snapshot store
func (s *Snap) Sync() (err error) {
	if err := s.dataFile.Sync(); err != nil {
		return err
	}

	return nil
}

// Close releases resources
func (s *Snap) Close() (err error) {
	if err := s.dataFile.Close(); err != nil {
		return err
	}

	return nil
}

// writeSnapshot creates a snapshot on given path and returns created snapshot.
// This snapshot will have the complete index tree already loaded into ram.
func writeSnapshot(dir string, root *TNode) (s *Snap, err error) {
	// ! TODO create snapshot at given dir
	return nil, nil
}

// readSnapRoot decodes an index tree branch from a byte slice
// This can be used to read the index root level information.
func readSnapRoot(p []byte) (tree *TNode, info *SnapInfo, err error) {
	// ! TODO load tree root from a snapshot
	return nil, nil, nil
}

// readSnapData decodes an index tree branch from a byte slice
// This can be used to read the index root level information.
func readSnapData(p []byte) (tree *TNode, info *SnapInfo, err error) {
	// ! TODO load tree root from a snapshot
	return nil, nil, nil
}
