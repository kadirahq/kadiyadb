package index

import (
	"errors"
	"io"
	"path"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/kadirahq/go-tools/hybrid"
	"github.com/kadirahq/go-tools/segments"
	"github.com/kadirahq/go-tools/segments/segmmap"
)

const (
	// index file prefix when stored in append only log format
	// index files will be named "logs_0, logs_1, ..."
	prefixlogs = "logs_"

	// Size of the segment file
	// !IMPORTANT if this value changes, the database will not be able to use
	// older data. To avoid accidental changes, this value is hardcoded here.
	segszlogs = 1024 * 1024 * 20
)

var (
	// ErrShortWrite is returned when number of bytes written does not
	// match the number of bytes used with the write operation.
	ErrShortWrite = errors.New("bytes written != payload size")
)

// Logs stores index nodes as a log. This is done in order to immediately
// store the index node when writing data to the disk. This is significantly
// faster and safer when compared to creating and writing a index snapshot.
//
// Index Log File Format:
//
// [size-0][protobuf-marshalled-node-0]
// [size-1][protobuf-marshalled-node-1]
//
type Logs struct {
	logFile segments.Store
	nextID  int64
	nextOff int64
	iomutex *sync.Mutex
}

// NewLogs creates a log type index persister.
func NewLogs(dir string) (l *Logs, err error) {
	sfpath := path.Join(dir, prefixlogs)
	f, err := segmmap.New(sfpath, segszlogs)
	if err != nil {
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

	// If the index node can be written to a single segment file without breaking
	// its content, we can directly use a byte slice from the segment file.
	// Otherwise, we must write it to a temporary buffer and flush it later.
	var fast bool
	var buff []byte

	// protobuf size
	node := n.Node
	size := node.Size()
	sz64 := int64(size)
	full := sz64 + hybrid.SzInt64

	if err := l.logFile.Ensure(l.nextOff + full); err != nil {
		return err
	}

	p, err := l.logFile.SliceAt(full, l.nextOff)
	if err != nil {
		return err
	}

	if len(p) == int(full) {
		buff = p
		fast = true
	} else {
		buff = make([]byte, full)
	}

	// Write the node size to the buffer with hybrid
	hybrid.EncodeInt64(buff[:hybrid.SzInt64], &sz64)

	// Using protobuf MarshalTo for better performance
	if n, err := node.MarshalTo(buff[hybrid.SzInt64:]); err != nil {
		return err
	} else if n != size {
		return ErrShortWrite
	}

	if !fast {
		// If we were using a temporary buffer to marshal data,
		// it's time for it to go to its final destination!
		towrite := buff[:]
		for len(towrite) > 0 {
			n, err := l.logFile.WriteAt(towrite, l.nextOff)
			if err != nil {
				return err
			}

			towrite = towrite[n:]
		}
	}

	// next item offset
	l.nextOff += full

	return nil
}

// Load loads all index nodes from the log file and builds the index tree.
// It also sets values for its Logs.nextID and Logs.nextOff fields.
func (l *Logs) Load() (tree *TNode, err error) {
	l.iomutex.Lock()
	defer l.iomutex.Unlock()

	l.nextID = 0
	l.nextOff = 0

	if _, err := l.logFile.Seek(0, 0); err != nil {
		return nil, err
	}

	root := &Node{Fields: []string{}}
	tree = WrapNode(root)

	var leftover []byte
	var nextSize hybrid.Int64
	var skipSize bool

	for {
		d, err := l.logFile.Slice(segszlogs)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

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
					buff = append(leftover, d[off:off+need]...)
					off += need
					l.nextOff += need
					leftover = nil
				} else {
					buff = d[off : off+hybrid.SzInt64]
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
				buff = append(leftover, d[off:off+need]...)
				off += need
				l.nextOff += need
				leftover = nil
			} else {
				buff = d[off : off+size]
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

			tn := tree.Ensure(node.Fields)
			tn.Mutex.Lock()
			tn.Node = node
			tn.Mutex.Unlock()

			l.nextID++
		}
	}

	return tree, nil
}

// Sync syncs all log segment files
func (l *Logs) Sync() (err error) {
	if err := l.logFile.Sync(); err != nil {
		return err
	}

	return nil
}

// Close releases resources
func (l *Logs) Close() (err error) {
	if err := l.logFile.Close(); err != nil {
		return err
	}

	return nil
}
