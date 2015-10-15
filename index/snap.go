package index

import (
	"bufio"
	"errors"
	"io"
	"path"

	"github.com/kadirahq/go-tools/hybrid"
	"github.com/kadirahq/go-tools/segments"
	"github.com/kadirahq/go-tools/segments/segfile"
)

const (
	// index file prefix when stored in snapshot format
	// index files will be named "snap_0, snap_1, ..."
	prefixsnaproot = "snapr_"
	prefixsnapdata = "snapd_"

	// Size of the segment file
	// !IMPORTANT if this value changes, the database will not be able to use
	// older data. To avoid accidental changes, this value is hardcoded here.
	segszsnap = 1024 * 1024 * 20
)

var (
	// ErrNoSnap is returned when there's no snapshot available
	ErrNoSnap = errors.New("no snapshot available")
)

// Snap helps create and load index pre-built index trees from snapshot files.
// Index snapshots are read-only, any changes require a rebuild of the snapshot.
type Snap struct {
	RootNode *TNode
	branches map[string]*Offset
	dataFile segments.Store
}

// LoadSnap opens an index persister which stores pre-built index trees.
// When loading a index snapshot, only the top level of the tree is loaded.
// All other tree branches are loaded only when it's necessary (on request).
func LoadSnap(dir string) (s *Snap, err error) {
	segpathr := path.Join(dir, prefixsnaproot)
	segpathd := path.Join(dir, prefixsnapdata)

	rf, err := segfile.New(segpathr, segszsnap)
	if err != nil {
		return nil, err
	}

	root, branches, err := readSnapRoot(rf)
	if err != nil {
		return nil, err
	}

	if err := rf.Close(); err != nil {
		return nil, err
	}

	df, err := segfile.New(segpathd, segszsnap)
	if err != nil {
		return nil, err
	}

	s = &Snap{
		RootNode: root,
		branches: branches,
		dataFile: df,
	}

	return s, nil
}

// LoadBranch function loads a branch from the data memory map
func (s *Snap) LoadBranch(key string) (tree *TNode, err error) {
	return readSnapData(s.dataFile, s.branches[key])
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
func writeSnapshot(dir string, tree *TNode) (s *Snap, err error) {
	segpathr := path.Join(dir, prefixsnaproot)
	segpathd := path.Join(dir, prefixsnapdata)

	rf, err := segfile.New(segpathr, segszsnap)
	if err != nil {
		return nil, err
	}

	// can close this
	defer rf.Close()

	df, err := segfile.New(segpathd, segszsnap)
	if err != nil {
		return nil, err
	}

	brf := bufio.NewWriterSize(rf, 1e7)
	bdf := bufio.NewWriterSize(df, 1e7)
	branches := map[string]*Offset{}

	var offset int64
	var buffer []byte

	for name, tn := range tree.Children {
		size := tn.Size()
		sz64 := int64(size)

		if len(buffer) < size {
			buffer = make([]byte, size)
		}

		// slice to data size
		towrite := buffer[:size]

		_, err := tn.MarshalTo(towrite)
		if err != nil {
			return nil, err
		}

		for len(towrite) > 0 {
			n, err := bdf.Write(towrite)
			if err != nil {
				return nil, err
			}

			towrite = towrite[n:]
		}

		branches[name] = &Offset{offset, offset + sz64}
		offset += sz64
	}

	info := &SnapInfo{
		Branches: branches,
	}

	{
		size := info.Size()
		sz64 := int64(size)
		full := size + hybrid.SzInt64

		if len(buffer) < full {
			buffer = make([]byte, full)
		}

		towrite := buffer[:full]

		// prepend root info struct size to the buffer
		hybrid.EncodeInt64(towrite[:hybrid.SzInt64], &sz64)

		_, err := info.MarshalTo(towrite[hybrid.SzInt64:])
		if err != nil {
			return nil, err
		}

		for len(towrite) > 0 {
			n, err := brf.Write(towrite)
			if err != nil {
				return nil, err
			}

			towrite = towrite[n:]
		}
	}

	if err := bdf.Flush(); err != nil {
		return nil, err
	}
	if err := brf.Flush(); err != nil {
		return nil, err
	}

	s = &Snap{
		RootNode: tree,
		branches: branches,
		dataFile: df,
	}

	return s, nil
}

// readSnapRoot decodes an index tree branch from a byte slice
// This can be used to read the index root level information.
func readSnapRoot(r io.Reader) (tree *TNode, branches map[string]*Offset, err error) {
	buffer := make([]byte, hybrid.SzInt64)
	var offset int64

	for offset < hybrid.SzInt64 {
		n, err := r.Read(buffer[offset:])
		if err != nil {
			return nil, nil, err
		}

		offset += int64(n)
	}

	var size64 int64
	hybrid.DecodeInt64(buffer, &size64)

	if size64 == 0 {
		return nil, nil, ErrNoSnap
	}

	buffer = make([]byte, size64)
	offset = 0

	for offset < size64 {
		n, err := r.Read(buffer[offset:])
		if err != nil {
			return nil, nil, err
		}

		offset += int64(n)
	}

	info := &SnapInfo{}
	if err := info.Unmarshal(buffer); err != nil {
		return nil, nil, err
	}

	tree = WrapNode(nil)
	branches = info.Branches

	for name := range branches {
		tree.Children[name] = nil
	}

	return tree, branches, nil
}

// readSnapData decodes an index tree branch from a byte slice
// This can be used to read the index root level information.
func readSnapData(r io.ReaderAt, o *Offset) (tree *TNode, err error) {
	size64 := o.To - o.From
	buffer := make([]byte, size64)
	toread := buffer[:]

	var offset int64
	for len(toread) > 0 {
		n, err := r.ReadAt(toread, o.From+offset)
		if err != nil {
			return nil, err
		}

		toread = toread[n:]
		offset += int64(n)
	}

	tree = &TNode{}
	if err := tree.Unmarshal(buffer); err != nil {
		return nil, err
	}

	return tree, nil
}
