package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	goerr "github.com/go-errors/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/kadirahq/go-tools/fsutils"
	"github.com/kadirahq/go-tools/logger"
	"github.com/kadirahq/go-tools/segfile"
)

const (
	// LogDataFilePrefix will be prefixed to log data segment files
	LogDataFilePrefix = "index_"

	// SnapRootFilePrefix will be prefixed to snapshot root segment files
	SnapRootFilePrefix = "index_snap_root_"

	// SnapDataFilePrefix will be prefixed to snapshot data segment files
	SnapDataFilePrefix = "index_snap_data_"
)

var (
	// ErrOptions is returned when options have missing or invalid fields.
	ErrOptions = errors.New("invalid or missing options")

	// ErrNoIndex is returned when the index is not found on disk (read-only).
	ErrNoIndex = errors.New("requested index is not available on disk")

	// ErrROnly is returned when a write is requested on a read only index
	ErrROnly = errors.New("cannot write on a read only index")

	// ErrNoWild is returned when user provides wildcard fields.
	// Occurs when requesting a specific index entry using One method.
	// Also occurs when user tries to Put an index entry with wildcards.
	ErrNoWild = errors.New("wildcards are not allowed in One requests")

	// ErrNoItem is returned when the requested element is not available
	// Only happens when requesting a specific index entry using One method.
	ErrNoItem = errors.New("requested item is not available in the index")

	// ErrExists is returned when the index element already exists on disk
	// This error can occur when an index item is added with same fields
	ErrExists = errors.New("the item already exists the index")

	// ErrCorrupt is returned when the segfile is corrupt
	ErrCorrupt = errors.New("segfile is corrupt")

	// ErrClosed is returned when the resource is closed
	ErrClosed = errors.New("cannot use closed resource")

	// NoValue is stored when there's no value
	// It has the maximum possible value for uint32
	NoValue = ^uint32(0)

	// Logger from which all index loggers are made
	Logger = logger.New("index")
)

// Options for new index
type Options struct {
	// directory to store index files
	Path string

	// no writes allowed
	ROnly bool
}

// Index is a simple data structure to store binary data and associate it
// with a number of fields (string). Data can be stored on both leaf nodes
// and intermediate nodes.
type Index interface {
	// Put adds a new node into the tree and saves it to the disk.
	// Intermediate nodes are created in memory if not available.
	Put(fields []string, value uint32) (err error)

	// One is used to query a specific node from the tree.
	// returns ErrNoItem if the node is not available.
	// (or has children doesn't have a value for itself)
	One(fields []string) (item *Item, err error)

	// Get queries a sub-tree from the index with all child nodes.
	// An empty string is considered as the wildcard value (match all).
	// Result can be filtered by setting fields after the wildcard field.
	Get(fields []string) (items []*Item, err error)

	// Metrics returns performance metrics
	// It also resets all counters
	Metrics() (m *Metrics)

	// Sync synchronizes writes
	Sync() (err error)

	// Close cleans up stuff, releases resources and closes the index.
	Close() (err error)
}

// Metrics contains runtime metrics
type Metrics struct {
	// TODO code!
}

type index struct {
	// segmented file with block data (logs)
	logData segfile.File

	// segmented file with block data (snapshot - root nodes)
	snapRoot segfile.File

	// segmented file with block data (snapshot - branches)
	snapData segfile.File

	// start/end offsets of branches
	offsets map[string]*offsets

	// file path
	path string

	// tree root node
	root *node

	// add mutex to control adds
	mutex sync.RWMutex

	// log with index info
	logger *logger.Logger

	// set to true when the file is closed
	closed bool

	// set to true when the file is read only
	ronly bool
}

type node struct {
	*Item

	// children nodes
	children map[string]*node
}

type offsets struct {
	// branch start
	start uint32

	// branch end
	end uint32
}

// New function creates an new `Index` with given `Options`
// It also loads tree nodes from the disk and builds the tree in memory.
// Finally space is allocated in disk if necessary to store mote nodes.
func New(options *Options) (idx Index, err error) {
	defer Logger.Time(time.Now(), time.Second, "New")
	// validate options
	if options == nil ||
		options.Path == "" {
		return nil, goerr.Wrap(ErrOptions, 0)
	}

	// check whether index directory exists
	if options.ROnly {
		err = os.Chdir(options.Path)

		if os.IsNotExist(err) {
			err = ErrNoIndex
		}

		if err != nil {
			return nil, goerr.Wrap(err, 0)
		}
	}

	root := &node{
		Item:     &Item{},
		children: make(map[string]*node),
	}

	i := &index{
		root:    root,
		path:    options.Path,
		ronly:   options.ROnly,
		logger:  Logger.New(options.Path),
		offsets: make(map[string]*offsets),
	}

	// Always use log files for read-write mode.
	if !options.ROnly {
		i.logData, err = segfile.New(&segfile.Options{
			Path:      options.Path,
			Prefix:    LogDataFilePrefix,
			ReadOnly:  false,
			MemoryMap: true,
		})

		if err != nil {
			return nil, goerr.Wrap(err, 0)
		}

		// if loading log data fails return error after closing the file.
		if err := i.loadLogfile(); err != nil {

			if err := i.Close(); err != nil {
				i.logger.Error(err)
			}

			return nil, goerr.Wrap(err, 0)
		}

		// seek to file end to do future writes
		if _, err := i.logData.Seek(0, 2); err != nil {

			if err := i.Close(); err != nil {
				i.logger.Error(err)
			}

			return nil, goerr.Wrap(err, 0)
		}

		return i, nil
	}

	var errRoot, errData error

	i.snapRoot, errRoot = segfile.New(&segfile.Options{
		Path:     options.Path,
		Prefix:   SnapRootFilePrefix,
		ReadOnly: true,
	})

	i.snapData, errData = segfile.New(&segfile.Options{
		Path:     options.Path,
		Prefix:   SnapDataFilePrefix,
		ReadOnly: true,
	})

	// load snapshot data if both files are opened successfully
	if errRoot == nil && errData == nil {
		// if loading data from snapshot files was successful
		// there's no need to load data from log files again.
		if err := i.loadSnapshot(); err == nil {
			return i, nil
		}
	}

	if i.snapRoot != nil {
		if err = i.snapRoot.Close(); err != nil {
			i.logger.Error(err)
		}

		i.snapRoot = nil
	}

	if i.snapData != nil {
		if err = i.snapData.Close(); err != nil {
			i.logger.Error(err)
		}

		i.snapData = nil
	}

	// loading nodes from snapshot failed.
	// try to load from log files and create snapshot files.
	i.logData, err = segfile.New(&segfile.Options{
		Path:     options.Path,
		Prefix:   LogDataFilePrefix,
		ReadOnly: true,
	})

	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	// if loading log data fails return error after closing all files.
	if err := i.loadLogfile(); err != nil {

		if err := i.Close(); err != nil {
			i.logger.Error(err)
		}

		return nil, goerr.Wrap(err, 0)
	}

	// loading nodes from log file was successful.
	// attmept to create snapshot files.
	if err := i.saveSnapshot(); err != nil {
		i.logger.Error(err)
	}

	return i, nil
}

func (i *index) Put(fields []string, value uint32) (err error) {
	defer Logger.Time(time.Now(), time.Second, "index.Put")
	if i.ronly {
		return goerr.Wrap(ErrROnly, 0)
	}

	for _, f := range fields {
		if f == "" {
			return goerr.Wrap(ErrNoWild, 0)
		}
	}

	if _, err := i.One(fields); !goerr.Is(err, ErrNoItem) {
		return goerr.Wrap(ErrExists, 0)
	}

	n := &node{
		Item:     &Item{Fields: fields, Value: value},
		children: make(map[string]*node),
	}

	if err = i.store(n); err != nil {
		return goerr.Wrap(err, 0)
	}

	// index item should be saved before adding it to the in memory index
	// otherwise index may miss some items when the server restarts
	err = i.append(n)
	if err != nil {
		return goerr.Wrap(err, 0)
	}

	return nil
}

func (i *index) One(fields []string) (item *Item, err error) {
	defer Logger.Time(time.Now(), time.Second, "index.One")
	node := i.root

	i.mutex.RLock()
	defer i.mutex.RUnlock()

	var ok bool
	for _, v := range fields {
		if v == "" {
			return nil, goerr.Wrap(ErrNoWild, 0)
		}

		if node.children == nil {
			// loading a branch needs a full lock
			// release the write lock to get that
			i.mutex.RUnlock()
			err := i.loadBranch(node)

			// unlocked with defer
			i.mutex.RLock()

			if err != nil {
				return nil, goerr.Wrap(err, 0)
			}

			// index has changed
			// restart the query
			return i.One(fields)
		}

		if node, ok = node.children[v]; !ok {
			return nil, goerr.Wrap(ErrNoItem, 0)
		}
	}

	if node.Item.Value == NoValue {
		return nil, goerr.Wrap(ErrNoItem, 0)
	}

	return node.Item, nil
}

func (i *index) Get(fields []string) (items []*Item, err error) {
	defer Logger.Time(time.Now(), time.Second, "index.Get")
	needsFilter := false

	i.mutex.RLock()
	defer i.mutex.RUnlock()

	root := i.root
	nfields := len(fields)
	var ok bool

	for j, v := range fields {
		if v == "" {
			// check whether we have any non-empty fields below
			for k := nfields - 1; k >= j; k-- {
				if fields[k] != "" {
					needsFilter = true
				}
			}

			break
		}

		if root.children == nil {
			// loading a branch needs a full lock
			// release the write lock to get that
			i.mutex.RUnlock()
			err := i.loadBranch(root)

			// unlocked with defer
			i.mutex.RLock()

			if err != nil {
				return nil, goerr.Wrap(err, 0)
			}

			// index has changed
			// restart the query
			return i.Get(fields)
		}

		if root, ok = root.children[v]; !ok {
			items = make([]*Item, 0)
			return items, nil
		}
	}

	items, err = i.getNodes(root)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	if !needsFilter {
		return items, nil
	}

	filtered := items[:0]

outer:
	for _, item := range items {
		for j := range item.Fields {
			if fields[j] != "" && fields[j] != item.Fields[j] {
				continue outer
			}
		}

		filtered = append(filtered, item)
	}

	return filtered, nil
}

func (i *index) Metrics() (m *Metrics) {
	// TODO code!
	return &Metrics{}
}

func (i *index) Sync() (err error) {
	if i.closed {
		i.logger.Error(ErrClosed)
		return nil
	}

	if i.ronly {
		return nil
	}

	if err := i.logData.Sync(); err != nil {
		return goerr.Wrap(err, 0)
	}

	return nil
}

func (i *index) Close() (err error) {
	defer Logger.Time(time.Now(), time.Second, "index.Close")
	if i.closed {
		i.logger.Error(ErrClosed)
		return nil
	}

	if !i.ronly {
		if err := i.saveSnapshot(); err != nil {
			i.logger.Error(err)
		}
	}

	if i.logData != nil {
		if err = i.logData.Close(); err != nil {
			i.logger.Error(err)
		}
	}

	if i.snapRoot != nil {
		if err = i.snapRoot.Close(); err != nil {
			i.logger.Error(err)
		}
	}

	if i.snapData != nil {
		if err = i.snapData.Close(); err != nil {
			i.logger.Error(err)
		}
	}

	if err != nil {
		return goerr.Wrap(err, 0)
	}

	return nil
}

// save method serializes and saves the node to disk
// format: [size uint32 | payload []byte]
func (i *index) store(nd *node) (err error) {
	defer Logger.Time(time.Now(), time.Second, "index.store")
	buffer := bytes.NewBuffer(nil)
	itemBytes, err := proto.Marshal(nd.Item)
	if err != nil {
		return goerr.Wrap(err, 0)
	}

	itemSize := uint32(len(itemBytes))
	if err = binary.Write(buffer, binary.LittleEndian, itemSize); err != nil {
		return goerr.Wrap(err, 0)
	}

	n, err := buffer.Write(itemBytes)
	if err != nil {
		return goerr.Wrap(err, 0)
	} else if uint32(n) != itemSize {
		return goerr.Wrap(fsutils.ErrWriteSz, 0)
	}

	data := buffer.Bytes()
	m, err := i.logData.Write(data)
	if err != nil {
		return goerr.Wrap(err, 0)
	} else if uint32(m) != itemSize+4 {
		return goerr.Wrap(fsutils.ErrWriteSz, 0)
	}

	return nil
}

// add adds a new node to the tree.
// intermediate nodes will be created if not available.
// If a node already exists, its value will be updated.
// This can happen when an intermediate node is set after setting
// one of its child nodes are set.
func (i *index) append(n *node) (err error) {
	defer Logger.Time(time.Now(), time.Second, "index.append")
	// make sure the branch is loaded
	i.mutex.RLock()
	firstField := n.Fields[0]
	firstNode, ok := i.root.children[firstField]
	i.mutex.RUnlock()

	if ok && firstNode.children == nil {
		err = i.loadBranch(firstNode)
		if err != nil {
			return goerr.Wrap(err, 0)
		}
	}

	i.mutex.Lock()
	defer i.mutex.Unlock()

	// start from the root
	root := i.root
	count := len(n.Fields)
	mfields := n.Fields[:count-1]

	// traverse through the tree by node fields
	// creates missing nodes upto the leaf node
	for j, field := range mfields {
		newRoot, ok := root.children[field]

		if !ok {
			// fields upto this node of the tree
			newRootFields := n.Fields[0 : j+1]
			newRoot = &node{
				Item:     &Item{Fields: newRootFields, Value: NoValue},
				children: make(map[string]*node),
			}

			root.children[field] = newRoot
		}

		root = newRoot
	}

	// add leaf node at the end if does not exist
	// if a node already exists, update its value
	field := n.Fields[count-1]
	leaf, ok := root.children[field]
	if ok {
		leaf.Item.Value = n.Item.Value
	} else {
		root.children[field] = n
	}

	return nil
}

// getNodes recursively collects all nodes inside a branch
func (i *index) getNodes(root *node) (items []*Item, err error) {
	defer Logger.Time(time.Now(), time.Second, "index.getNodes")
	items = make([]*Item, 0)

	if root.Value != NoValue {
		items = append(items, root.Item)
	}

	if root.children == nil {
		err = i.loadBranch(root)
		if err != nil {
			return nil, goerr.Wrap(err, 0)
		}
	}

	for _, nd := range root.children {
		res, err := i.getNodes(nd)
		if err != nil {
			return nil, goerr.Wrap(err, 0)
		}

		items = append(items, res...)
	}

	return items, nil
}

func (i *index) loadBranch(n *node) (err error) {
	defer Logger.Time(time.Now(), time.Second, "index.loadBranch")
	err = i.snapData.Reset()
	if err != nil {
		return goerr.Wrap(err, 0)
	}

	n.children = make(map[string]*node)
	firstField := n.Fields[0]
	offsets := i.offsets[firstField]
	dataSize := offsets.end - offsets.start

	// read data from file in one attempt
	dataBuff := make([]byte, dataSize)
	num, err := i.snapData.ReadAt(dataBuff, int64(offsets.start))
	if err != nil {
		return goerr.Wrap(err, 0)
	} else if uint32(num) != dataSize {
		return goerr.Wrap(ErrCorrupt, 0)
	}

	var buffer = bytes.NewBuffer(dataBuff)
	var itemBuff = []byte{}

	for {
		var size uint32

		err = binary.Read(buffer, binary.LittleEndian, &size)
		if err != nil && err != io.EOF {
			return goerr.Wrap(err, 0)
		} else if err == io.EOF {
			// io.EOF file will occur when we're read exactly up to file end.
			// This is a very rare incident because file is preallocated.
			break
		}

		// Size will be zero when pre allocated data is read.
		// Assume all data items were read at this point.
		if size == 0 {
			break
		}

		if uint32(cap(itemBuff)) < size {
			itemBuff = make([]byte, size)
		}

		itemData := itemBuff[0:size]
		n, err := buffer.Read(itemData)
		if err != nil {
			return goerr.Wrap(err, 0)
		} else if uint32(n) != size {
			return goerr.Wrap(ErrCorrupt, 0)
		}

		item := &Item{}
		err = proto.Unmarshal(itemData, item)
		if err != nil {
			return goerr.Wrap(err, 0)
		}

		nd := &node{
			Item:     item,
			children: make(map[string]*node),
		}

		err = i.append(nd)
		if err != nil {
			return goerr.Wrap(err, 0)
		}
	}

	return nil
}

func (i *index) loadSnapshot() (err error) {
	defer Logger.Time(time.Now(), time.Second, "index.loadSnapshot")
	err = i.snapRoot.Reset()
	if err != nil {
		return goerr.Wrap(err, 0)
	}

	var (
		fileSize   = i.snapRoot.Size()
		dataBuff   []byte
		bytesRead  int64
		footerSize uint32 = 8
	)

	for bytesRead < fileSize {
		// bytes available after reading item header
		bytesAvailable := uint32(fileSize - bytesRead - 4)

		var itemSize uint32
		err = binary.Read(i.snapRoot, binary.LittleEndian, &itemSize)
		if err != nil && err != io.EOF {
			return goerr.Wrap(err, 0)
		} else if err == io.EOF || itemSize == 0 {
			// io.EOF file will occur when we're read exactly up to file end.
			// This is a very rare incident because file is preallocated.
			// As we always preallocate with zeroes, itemSize will be zero.
			break
		} else if itemSize+footerSize > bytesAvailable {
			// If we came to this point in this if-else ladder it means that file
			// contains an itemSize but does not have enough bytes left.
			return goerr.Wrap(ErrCorrupt, 0)
		}

		if uint32(cap(dataBuff)) < itemSize {
			dataBuff = make([]byte, itemSize)
		}

		itemData := dataBuff[0:itemSize]
		n, err := i.snapRoot.Read(itemData)
		if err != nil {
			return goerr.Wrap(err, 0)
		} else if uint32(n) != itemSize {
			return goerr.Wrap(ErrCorrupt, 0)
		}

		var startOffset uint32
		err = binary.Read(i.snapRoot, binary.LittleEndian, &startOffset)
		if err != nil {
			return goerr.Wrap(err, 0)
		}

		var endOffset uint32
		err = binary.Read(i.snapRoot, binary.LittleEndian, &endOffset)
		if err != nil {
			return goerr.Wrap(err, 0)
		}

		item := &Item{}
		err = proto.Unmarshal(itemData, item)
		if err != nil {
			return goerr.Wrap(err, 0)
		}

		nd := &node{
			Item:     item,
			children: nil,
		}

		firstField := item.Fields[0]
		i.offsets[firstField] = &offsets{start: startOffset, end: endOffset}

		err = i.append(nd)
		if err != nil {
			return goerr.Wrap(err, 0)
		}

		bytesRead += 4 + int64(itemSize+footerSize)
	}

	return nil
}

func (i *index) saveSnapshot() (err error) {
	defer Logger.Time(time.Now(), time.Second, "index.saveSnapshot")
	if i.snapRoot == nil {
		i.snapRoot, err = segfile.New(&segfile.Options{
			Path:   i.path,
			Prefix: SnapRootFilePrefix,
		})
	} else {
		err = i.snapRoot.Clear()
	}

	if err != nil {
		return goerr.Wrap(err, 0)
	}

	if i.snapData == nil {
		i.snapData, err = segfile.New(&segfile.Options{
			Path:   i.path,
			Prefix: SnapDataFilePrefix,
		})
	} else {
		err = i.snapData.Clear()
	}

	if err != nil {
		return goerr.Wrap(err, 0)
	}

	for _, root := range i.root.children {
		// get data file offset at start
		soff := uint32(i.snapData.Size())

		items, err := i.getNodes(root)
		if err != nil {
			return goerr.Wrap(err, 0)
		}

		for _, item := range items {
			itemBytes, err := proto.Marshal(item)
			if err != nil {
				return goerr.Wrap(err, 0)
			}

			itemSize := uint32(len(itemBytes))
			err = binary.Write(i.snapData, binary.LittleEndian, itemSize)
			if err != nil {
				return goerr.Wrap(err, 0)
			}

			n, err := i.snapData.Write(itemBytes)
			if err != nil {
				return goerr.Wrap(err, 0)
			} else if uint32(n) != itemSize {
				return goerr.Wrap(fsutils.ErrWriteSz, 0)
			}
		}

		// get data file offset after writing
		eoff := uint32(i.snapData.Size())

		item := root.Item
		itemBytes, err := proto.Marshal(item)
		if err != nil {
			return goerr.Wrap(err, 0)
		}

		itemSize := uint32(len(itemBytes))
		err = binary.Write(i.snapRoot, binary.LittleEndian, itemSize)
		if err != nil {
			return goerr.Wrap(err, 0)
		}

		n, err := i.snapRoot.Write(itemBytes)
		if err != nil {
			return goerr.Wrap(err, 0)
		} else if uint32(n) != itemSize {
			return goerr.Wrap(fsutils.ErrWriteSz, 0)
		}

		err = binary.Write(i.snapRoot, binary.LittleEndian, soff)
		if err != nil {
			return goerr.Wrap(err, 0)
		}

		err = binary.Write(i.snapRoot, binary.LittleEndian, eoff)
		if err != nil {
			return goerr.Wrap(err, 0)
		}
	}

	return nil
}

func (i *index) loadLogfile() (err error) {
	defer Logger.Time(time.Now(), time.Second, "index.loadLogfile")
	buffer := i.logData
	buffSize := buffer.Size()

	sizeData := make([]byte, 4)
	sizeBuff := bytes.NewBuffer(nil)
	dataBuff := make([]byte, 1024)

	var itemSize uint32
	var itemData []byte
	var offset int64
	var items int64

	for offset+4 < buffSize {
		n, err := buffer.ReadAt(sizeData, offset)
		if err != nil && err != io.EOF {
			return goerr.Wrap(err, 0)
		} else if err == io.EOF {
			// no more data to read
			break
		} else if n != 4 {
			return goerr.Wrap(fsutils.ErrReadSz, 0)
		}

		// update offset
		offset += 4

		sizeBuff.Reset()
		if n, err := sizeBuff.Write(sizeData); err != nil {
			return goerr.Wrap(err, 0)
		} else if n != 4 {
			return goerr.Wrap(fsutils.ErrReadSz, 0)
		}

		err = binary.Read(sizeBuff, binary.LittleEndian, &itemSize)
		if err != nil {
			return goerr.Wrap(err, 0)
		} else if itemSize == 0 {
			// no more items
			break
		}

		if itemSize > uint32(len(dataBuff)) {
			dataBuff = make([]byte, itemSize)
		}

		itemData = dataBuff[:itemSize]
		if n, err := buffer.ReadAt(itemData, offset); err != nil {
			return goerr.Wrap(err, 0)
		} else if uint32(n) != itemSize {
			return goerr.Wrap(fsutils.ErrReadSz, 0)
		}

		// update offset
		offset += int64(itemSize)

		// TODO remove
		items++

		item := &Item{}
		err = proto.Unmarshal(itemData, item)
		if err != nil {
			return goerr.Wrap(err, 0)
		}

		nd := &node{
			Item:     item,
			children: make(map[string]*node),
		}

		err = i.append(nd)
		if err != nil {
			return goerr.Wrap(err, 0)
		}
	}

	return nil
}
