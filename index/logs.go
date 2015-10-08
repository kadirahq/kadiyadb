package index

import (
	"errors"
	"path"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/kadirahq/go-tools/hybrid"
	"github.com/kadirahq/go-tools/segmap"
)

const (
	prefixlogs = "logs_"
	segszlogs  = 1024 * 1024 * 20
)

// Logs stores index nodes
type Logs struct {
	logFile *segmap.Store
	nextID  int64
	nextOff int64
	iomutex *sync.Mutex
}

// NewLogs creates a log type index persister.
func NewLogs(dir string) (l *Logs, err error) {
	sfpath := path.Join(dir, prefixlogs)
	f, err := segmap.New(sfpath, segszlogs)
	if err != nil {
		return nil, err
	}

	if err := f.LoadAll(); err != nil {
		return nil, err
	}

	if err := f.Lock(); err != nil {
		return nil, err
	}

	l = &Logs{
		logFile: f,
		nextID:  0,
		nextOff: 0,
		iomutex: &sync.Mutex{},
	}

	return l, nil
}

// Store appends a node to the index log file and updates ID and Offset fields.
func (l *Logs) Store(n *TNode) (err error) {
	l.iomutex.Lock()
	defer l.iomutex.Unlock()

	// ignore all children nodes
	var fast bool
	var buff []byte

	// protobuf size
	node := n.Node
	size := node.Size()
	sz64 := int64(size)

	// most of the times it's possible to write directly to the memory map
	// instead of using an intermediate buffer. This can reduce garbage.
	chunks, err := l.logFile.ZReadAt(sz64, l.nextOff)
	if err != nil {
		return err
	}

	// If the target location is in a single file, it can be written directly
	// otherwise we have to use a temporary buffer to use with MarshalTo method.
	if len(chunks) == 1 {
		buff = chunks[0]
		fast = true
	} else {
		buff = make([]byte, size)
	}

	if n, err := node.MarshalTo(buff); err != nil {
		return err
	} else if n != size {
		// TODO return shared error
		return errors.New("")
	}

	if !fast {
		// If we were using a temporary buffer to marshal data,
		// it's time for it to go to its final destination!
		if n, err := l.logFile.WriteAt(buff, l.nextOff); err != nil {
			return err
		} else if n != size {
			// TODO return shared error
			return errors.New("")
		}
	}

	l.nextOff += sz64

	return nil
}

// Load loads all index nodes from the log file and builds the index tree.
// It also sets values for its Logs.nextID and Logs.nextOff fields.
func (l *Logs) Load() (tree *TNode, err error) {
	l.iomutex.Lock()
	defer l.iomutex.Unlock()

	l.nextID = 0
	l.nextOff = 0

	// memory copy operations can cause unnecessary cpu usage and latency
	// in order to avoid that, use the ZReadAt method of segmap.Store struct.
	// The downside is that the data is returned as a slice os byte slices
	// instead of one large byte slice when multiple memory maps are used.
	datasz := int64(l.logFile.Length() * segszlogs)
	chunks, err := l.logFile.ZReadAt(datasz, 0)
	if err != nil {
		return nil, err
	}

	root := &Node{Fields: []string{}}
	tree = WrapNode(root)

	var leftover []byte
	var nextSize hybrid.Int64
	var skipSize bool

	// TODO explain how this is done
	for _, d := range chunks {
		csz := int64(len(d))
		var off int64

		for {
			var buff []byte

			if skipSize {
				// read the size from previous slice
				// skip to reading the index node
				skipSize = false
			} else {
				// not enough bytes to read size
				if csz-off < hybrid.SzInt64 {
					leftover = d[off:]
					break
				}

				if leftover != nil {
					need := hybrid.SzInt64 - int64(len(leftover))
					buff = append(leftover, d[:need]...)
					off += need
					l.nextOff += need
					leftover = nil
				} else {
					buff = d[:hybrid.SzInt64]
					off += hybrid.SzInt64
					l.nextOff += hybrid.SzInt64
				}

				nextSize.Read(buff)
			}

			size := *nextSize.Value

			// node more data
			if size == 0 {
				break
			}

			// not enough bytes to read node
			if csz-off < size {
				skipSize = true
				leftover = d[off:]
				l.nextOff += off
				break
			}

			if leftover != nil {
				need := size - int64(len(leftover))
				buff = append(leftover, d[:need]...)
				off += need
				l.nextOff += need
				leftover = nil
			} else {
				buff = d[:size]
				off += size
				l.nextOff += size
			}

			node := &Node{}
			if err := proto.Unmarshal(buff, node); err != nil {
				return nil, err
			}

			if err := node.Validate(); err != nil {
				return nil, err
			}

			l.nextID++
			tree.Ensure(node.Fields)
		}
	}

	return tree, nil
}

// Sync syncs the log store
func (l *Logs) Sync() (err error) {
	if err := l.logFile.Sync(); err != nil {
		return err
	}

	return nil
}

// Close closes the log store
func (l *Logs) Close() (err error) {
	if err := l.logFile.Close(); err != nil {
		return err
	}

	return nil
}
