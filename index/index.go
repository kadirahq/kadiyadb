package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/gogo/protobuf/proto"
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
	// ErrClose is returned when close is closed multiple times
	ErrClose = errors.New("close called multiple times")

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

	// ErrWrite is returned when bytes written is not equal to data size
	ErrWrite = errors.New("bytes written != data size")

	// ErrRead is returned when bytes read is not equal to data size
	ErrRead = errors.New("bytes read != data size")

	// ErrCorrupt is returned when the segfile is corrupt
	ErrCorrupt = errors.New("segfile is corrupt")

	// NoValue is stored when there's no value
	// It has the maximum possible value for uint32
	NoValue = ^uint32(0)

	// Logger logs stuff
	Logger = logger.New("INDEX")
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
	// validate options
	if options == nil ||
		options.Path == "" {
		Logger.Trace(ErrOptions)
		return nil, ErrOptions
	}

	// check whether index directory exists
	if options.ROnly {
		err = os.Chdir(options.Path)

		if os.IsNotExist(err) {
			err = ErrNoIndex
		}

		if err != nil {
			Logger.Trace(err)
			return nil, err
		}
	}

	root := &node{
		Item:     &Item{},
		children: make(map[string]*node),
	}

	i := &index{
		root:  root,
		path:  options.Path,
		ronly: options.ROnly,
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
			Logger.Trace(err)
			return nil, err
		}

		// if loading log data fails return error after closing the file.
		if err := i.loadLogfile(); err != nil {
			Logger.Trace(err)

			if err := i.Close(); err != nil {
				Logger.Error(err)
			}

			return nil, err
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
			Logger.Error(err)
		}

		i.snapRoot = nil
	}

	if i.snapData != nil {
		if err = i.snapData.Close(); err != nil {
			Logger.Error(err)
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
		Logger.Trace(err)
		return nil, err
	}

	// if loading log data fails return error after closing all files.
	if err := i.loadLogfile(); err != nil {
		Logger.Trace(err)

		if err := i.Close(); err != nil {
			Logger.Error(err)
		}

		return nil, err
	}

	// loading nodes from log file was successful.
	// attmept to create snapshot files.
	if err := i.saveSnapshot(); err != nil {
		Logger.Error(err)
	}

	return i, nil
}

func (i *index) Put(fields []string, value uint32) (err error) {
	if i.ronly {
		Logger.Trace(ErrROnly)
		return ErrROnly
	}

	for _, f := range fields {
		if f == "" {
			Logger.Trace(ErrNoWild)
			return ErrNoWild
		}
	}

	_, err = i.One(fields)
	if err != ErrNoItem {
		Logger.Trace(ErrExists)
		return ErrExists
	}

	n := &node{
		Item:     &Item{Fields: fields, Value: value},
		children: make(map[string]*node),
	}

	err = i.store(n)
	if err != nil {
		Logger.Trace(err)
		return err
	}

	// index item should be saved before adding it to the in memory index
	// otherwise index may miss some items when the server restarts
	err = i.append(n)
	if err != nil {
		Logger.Trace(err)
		return err
	}

	return nil
}

func (i *index) One(fields []string) (item *Item, err error) {
	node := i.root

	i.mutex.RLock()
	defer i.mutex.RUnlock()

	var ok bool
	for _, v := range fields {
		if v == "" {
			Logger.Trace(ErrNoWild)
			return nil, ErrNoWild
		}

		if node, ok = node.children[v]; !ok {
			Logger.Trace(ErrNoItem)
			return nil, ErrNoItem
		}
	}

	if node.Item.Value == NoValue {
		Logger.Trace(ErrNoItem)
		return nil, ErrNoItem
	}

	return node.Item, nil
}

func (i *index) Get(fields []string) (items []*Item, err error) {
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
			err = i.loadBranch(root)
			if err != nil {
				Logger.Trace(err)
				return nil, err
			}
		}

		if root, ok = root.children[v]; !ok {
			items = make([]*Item, 0)
			return items, nil
		}
	}

	items, err = i.getNodes(root)
	if err != nil {
		Logger.Trace(err)
		return nil, err
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
		Logger.Error(ErrClose)
		return nil
	}

	if i.ronly {
		return nil
	}

	err = i.logData.Sync()
	if err != nil {
		Logger.Trace(err)
		return err
	}

	return nil
}

func (i *index) Close() (err error) {
	if i.closed {
		Logger.Error(ErrClose)
		return nil
	}

	if i.logData != nil {
		if err = i.logData.Close(); err != nil {
			Logger.Error(err)
		}
	}

	if i.snapRoot != nil {
		if err = i.snapRoot.Close(); err != nil {
			Logger.Error(err)
		}
	}

	if i.snapData != nil {
		if err = i.snapData.Close(); err != nil {
			Logger.Error(err)
		}
	}

	return err
}

// save method serializes and saves the node to disk
// format: [size uint32 | payload []byte]
func (i *index) store(nd *node) (err error) {
	itemBytes, err := proto.Marshal(nd.Item)
	if err != nil {
		Logger.Trace(err)
		return err
	}

	itemSize := uint32(len(itemBytes))
	err = binary.Write(i.logData, binary.LittleEndian, itemSize)
	if err != nil {
		Logger.Trace(err)
		return err
	}

	n, err := i.logData.Write(itemBytes)
	if err != nil {
		Logger.Trace(err)
		return err
	} else if uint32(n) != itemSize {
		Logger.Trace(ErrWrite)
		return ErrWrite
	}

	return nil
}

// add adds a new node to the tree.
// intermediate nodes will be created if not available.
// If a node already exists, its value will be updated.
// This can happen when an intermediate node is set after setting
// one of its child nodes are set.
func (i *index) append(n *node) (err error) {

	// make sure the branch is loaded
	i.mutex.RLock()
	firstField := n.Fields[0]
	firstNode, ok := i.root.children[firstField]
	i.mutex.RUnlock()

	if ok && firstNode.children == nil {
		err = i.loadBranch(firstNode)
		if err != nil {
			Logger.Trace(err)
			return err
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
	items = make([]*Item, 0)

	if root.Value != NoValue {
		items = append(items, root.Item)
	}

	if root.children == nil {
		err = i.loadBranch(root)
		if err != nil {
			Logger.Trace(err)
			return nil, err
		}
	}

	for _, nd := range root.children {
		res, err := i.getNodes(nd)
		if err != nil {
			Logger.Trace(err)
			return nil, err
		}

		items = append(items, res...)
	}

	return items, nil
}

func (i *index) loadBranch(n *node) (err error) {
	err = i.snapData.Reset()
	if err != nil {
		Logger.Trace(err)
		return err
	}

	n.children = make(map[string]*node)
	firstField := n.Fields[0]
	offsets := i.offsets[firstField]
	dataSize := offsets.end - offsets.start

	// read data from file in one attempt
	dataBuff := make([]byte, dataSize)
	num, err := i.snapData.ReadAt(dataBuff, int64(offsets.start))
	if err != nil {
		Logger.Trace(err)
		return err
	} else if uint32(num) != dataSize {
		Logger.Trace(ErrCorrupt)
		return ErrCorrupt
	}

	var buffer = bytes.NewBuffer(dataBuff)
	var itemBuff = []byte{}

	for {
		var size uint32

		err = binary.Read(buffer, binary.LittleEndian, &size)
		if err != nil && err != io.EOF {
			Logger.Trace(err)
			return err
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
			Logger.Trace(err)
			return err
		} else if uint32(n) != size {
			Logger.Trace(ErrCorrupt)
			return ErrCorrupt
		}

		item := &Item{}
		err = proto.Unmarshal(itemData, item)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		nd := &node{
			Item:     item,
			children: make(map[string]*node),
		}

		err = i.append(nd)
		if err != nil {
			Logger.Trace(err)
			return err
		}
	}

	return nil
}

func (i *index) loadSnapshot() (err error) {
	err = i.snapRoot.Reset()
	if err != nil {
		Logger.Trace(err)
		return err
	}

	var (
		fileSize   = i.snapData.Size()
		dataBuff   []byte
		bytesRead  int64
		footerSize uint32 = 8
	)

	for bytesRead < fileSize {
		// bytes available after reading item header
		bytesAvailable := uint32(fileSize - bytesRead - 4)

		var itemSize uint32
		err = binary.Read(i.snapData, binary.LittleEndian, &itemSize)
		if err != nil && err != io.EOF {
			Logger.Trace(err)
			return err
		} else if err == io.EOF || itemSize == 0 {
			// io.EOF file will occur when we're read exactly up to file end.
			// This is a very rare incident because file is preallocated.
			// As we always preallocate with zeroes, itemSize will be zero.
			break
		} else if itemSize+footerSize > bytesAvailable {
			// If we came to this point in this if-else ladder it means that file
			// contains an itemSize but does not have enough bytes left.
			Logger.Trace(ErrCorrupt)
			return ErrCorrupt
		}

		if uint32(cap(dataBuff)) < itemSize {
			dataBuff = make([]byte, itemSize)
		}

		itemData := dataBuff[0:itemSize]
		n, err := i.snapData.Read(itemData)
		if err != nil {
			Logger.Trace(err)
			return err
		} else if uint32(n) != itemSize {
			Logger.Trace(ErrCorrupt)
			return ErrCorrupt
		}

		var startOffset uint32
		err = binary.Read(i.snapData, binary.LittleEndian, &startOffset)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		var endOffset uint32
		err = binary.Read(i.snapData, binary.LittleEndian, &endOffset)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		item := &Item{}
		err = proto.Unmarshal(itemData, item)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		nd := &node{
			Item:     item,
			children: nil,
		}

		firstField := item.Fields[0]
		i.offsets[firstField] = &offsets{start: startOffset, end: endOffset}

		err = i.append(nd)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		bytesRead += 4 + int64(itemSize+footerSize)
	}

	return nil
}

func (i *index) saveSnapshot() (err error) {

	if i.snapRoot == nil {
		i.snapRoot, err = segfile.New(&segfile.Options{
			Path:   i.path,
			Prefix: SnapRootFilePrefix,
		})
	} else {
		err = i.snapRoot.Clear()
	}

	if err != nil {
		Logger.Trace(err)
		return err
	}

	if i.snapData == nil {
		i.snapData, err = segfile.New(&segfile.Options{
			Path:   i.path,
			Prefix: SnapRootFilePrefix,
		})
	} else {
		err = i.snapData.Clear()
	}

	if err != nil {
		Logger.Trace(err)
		return err
	}

	for _, root := range i.root.children {
		// get data file offset at start
		soff := uint32(i.snapData.Size())

		items, err := i.getNodes(root)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		for _, item := range items {
			itemBytes, err := proto.Marshal(item)
			if err != nil {
				Logger.Trace(err)
				return err
			}

			itemSize := uint32(len(itemBytes))
			err = binary.Write(i.snapData, binary.LittleEndian, itemSize)
			if err != nil {
				Logger.Trace(err)
				return err
			}

			n, err := i.snapData.Write(itemBytes)
			if err != nil {
				Logger.Trace(err)
				return err
			} else if uint32(n) != itemSize {
				Logger.Trace(ErrWrite)
				return ErrWrite
			}
		}

		// get data file offset after writing
		eoff := uint32(i.snapData.Size())

		item := root.Item
		itemBytes, err := proto.Marshal(item)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		itemSize := uint32(len(itemBytes))
		err = binary.Write(i.snapRoot, binary.LittleEndian, itemSize)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		n, err := i.snapRoot.Write(itemBytes)
		if err != nil {
			Logger.Trace(err)
			return err
		} else if uint32(n) != itemSize {
			Logger.Trace(ErrWrite)
			return ErrWrite
		}

		err = binary.Write(i.snapRoot, binary.LittleEndian, soff)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		err = binary.Write(i.snapRoot, binary.LittleEndian, eoff)
		if err != nil {
			Logger.Trace(err)
			return err
		}
	}

	return nil
}

func (i *index) loadLogfile() (err error) {
	buffer := i.logData

	var dataBuff []byte
	var itemSize uint32

	for {
		err = binary.Read(buffer, binary.LittleEndian, &itemSize)
		if err != nil && err != io.EOF {
			Logger.Trace(err)
			return err
		} else if err == io.EOF || itemSize == 0 {
			// io.EOF file will occur when we're read exactly up to file end.
			// This is a very rare incident because file is preallocated.
			// As we always preallocate with zeroes, itemSize will be zero.
			break
		}

		if uint32(cap(dataBuff)) < itemSize {
			dataBuff = make([]byte, itemSize)
		}

		itemData := dataBuff[0:itemSize]
		n, err := buffer.Read(itemData)
		if err != nil {
			Logger.Trace(err)
			return err
		} else if uint32(n) != itemSize {
			Logger.Trace(ErrRead)
			return ErrRead
		}

		item := &Item{}
		err = proto.Unmarshal(itemData, item)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		nd := &node{
			Item:     item,
			children: make(map[string]*node),
		}

		err = i.append(nd)
		if err != nil {
			Logger.Trace(err)
			return err
		}
	}

	if err = buffer.Reset(); err != nil {
		Logger.Trace(err)
		return err
	}

	return nil
}
