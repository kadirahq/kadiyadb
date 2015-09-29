package index

import (
	"errors"
	"path"

	"github.com/gogo/protobuf/proto"
	"github.com/kadirahq/go-tools/byteclone"
	"github.com/kadirahq/go-tools/segmmap"
)

const (
	prefixlogs = "logs_"
	segsz      = 1024 * 1024 * 20
)

// Logs helps to store index nodes in log format
type Logs struct {
	logFile *segmmap.Map
	nextID  int64
	nextOff int64
}

// NewLogs creates a log type index persister.
func NewLogs(dir string) (l *Logs, err error) {
	segpath := path.Join(dir, prefixlogs)
	f, err := segmmap.NewMap(segpath, segsz)
	if err != nil {
		return nil, err
	}

	if err := f.LoadAll(); err != nil {
		return nil, err
	}

	l = &Logs{
		logFile: f,
		nextID:  0,
		nextOff: 0,
	}

	return l, nil
}

// Store appends a node to the index log file and updates ID and Offset fields.
func (l *Logs) Store(n *TNode) (err error) {
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

	l.nextID++
	l.nextOff += int64(size)

	return nil
}

// Load loads all index nodes from the log file and builds the index tree.
// It also sets values for its Logs.nextID and Logs.nextOff fields.
func (l *Logs) Load() (tree *TNode, err error) {
	l.nextID = 0
	l.nextOff = 0

	// memory copy operations can cause unnecessary cpu usage and latency
	// in order to avoid that, use the ZReadAt method of segmmap.Map struct.
	// The downside is that the data is returned as a slice os byte slices
	// instead of one large byte slice when multiple memory maps are used.
	datasz := int64(len(l.logFile.Maps) * segsz)
	chunks, err := l.logFile.ZReadAt(datasz, 0)
	if err != nil {
		return nil, err
	}

	root := &Node{Fields: []string{}}
	tree = WrapNode(root)

	var leftover []byte
	var nextSize byteclone.Int64
	var skipSize bool

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
				if csz-off < byteclone.SzInt64 {
					leftover = d[off:]
					break
				}

				if leftover != nil {
					need := byteclone.SzInt64 - int64(len(leftover))
					buff = append(leftover, d[:need]...)
					off += need
					l.nextOff += need
					leftover = nil
				} else {
					buff = d[:byteclone.SzInt64]
					off += byteclone.SzInt64
					l.nextOff += byteclone.SzInt64
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

			tnode := WrapNode(node)
			if err := tnode.Validate(); err != nil {
				return nil, err
			}

			l.nextID++
			tree.Append(tnode)
		}
	}

	return tree, nil
}

// Close closes the log store
func (l *Logs) Close() (err error) {
	if err := l.logFile.Close(); err != nil {
		return err
	}

	return nil
}
