package index

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"github.com/kadirahq/kadiyadb/utils/logger"
	"github.com/kadirahq/kadiyadb/utils/mmap"
)

const (
	// PreallocSize is the number of bytes to pre-allocate when the indes
	// file runs out of space to store new elements. Space on disk is
	// allocated and memory mapped in order to increase write performance
	// 10 MB will be preallocated when the index file runs out of space.
	PreallocSize = 1024 * 1024 * 25

	// PreallocThresh is the minimum number of bytes to have in index memory map
	// before triggering a pre-allocation.
	PreallocThresh = 1024 * 1024 * 5

	// ItemHeaderSize is the number of bytes stored used to store metadata
	// with each Item (protobuf). Currently it only contains the Item size.
	ItemHeaderSize = 4
)

var (
	// ErrWrite is returned when number of bytes doesn't match data size
	ErrWrite = errors.New("number of bytes written doesn't match data size")

	// ErrLoad is returned when there's an error reading data from file
	ErrLoad = errors.New("there's an error reading items from the file")

	// ErrROnly is returned when a write is performed on a read-only index
	ErrROnly = errors.New("cannot add new items on a read-only index")

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

	// NoValue is stored when there's no value
	// It has the maximum possible value for uint32
	NoValue = ^uint32(0)

	// Logger logs stuff
	Logger = logger.New("INDEX")
)

type node struct {
	*Item                     // values
	children map[string]*node // children nodes
}

type offsets struct {
	start uint32
	end   uint32
}

// Options has parameters required for creating an `Index`
type Options struct {
	Path  string // path to index file
	ROnly bool   // the index is loaded only for reading
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

	// Close cleans up stuff, releases resources and closes the index.
	Close() (err error)
}

type index struct {
	opts       *Options            // options
	rootNode   *node               // tree root node
	offsets    map[string]*offsets // start/end offsets of branches
	mmapFile   *mmap.Map           // memory map of the file used to store the tree
	dataSize   int64               // number of bytes used in the memory map
	addMutex   *sync.Mutex         // mutex used to lock when new items are added
	allocMutex *sync.Mutex         // mutex used to lock when allocating space
	allocating bool                // indicates a pre-alloc is in progress
	metrics    *Metrics            // performance metrics
}

// New function creates an new `Index` with given `Options`
// It also loads tree nodes from the disk and builds the tree in memory.
// Finally space is allocated in disk if necessary to store mote nodes.
func New(options *Options) (_idx Index, err error) {
	metrics := &Metrics{}

	rootNode := &node{
		Item:     &Item{},
		children: make(map[string]*node),
	}

	idx := &index{
		opts:       options,
		rootNode:   rootNode,
		offsets:    map[string]*offsets{},
		addMutex:   &sync.Mutex{},
		allocMutex: &sync.Mutex{},
		metrics:    metrics,
	}

	if options.ROnly {
		if err := idx.loadSnapshot(); err == nil {
			return idx, nil
		}
	}

	idx.mmapFile, err = mmap.New(&mmap.Options{Path: options.Path})
	if err != nil {
		Logger.Trace(err)
		return nil, err
	}

	err = idx.mmapFile.Lock()
	if err != nil {
		Logger.Error(err)
	}

	if err := idx.load(); err != nil {
		Logger.Trace(err)

		if err := idx.mmapFile.Close(); err != nil {
			Logger.Error(err)
		}

		return nil, err
	}

	if options.ROnly {
		err = idx.mmapFile.Close()
		if err != nil {
			Logger.Error(err)
		}

		err = idx.saveSnapshot()
		if err != nil {
			Logger.Error(err)
		}
	} else {
		err = idx.preallocateIfNeeded()
		if err != nil {
			Logger.Trace(err)

			if err := idx.mmapFile.Close(); err != nil {
				Logger.Error(err)
			}

			return nil, err
		}
	}

	return idx, nil
}

func (idx *index) Put(fields []string, value uint32) (err error) {
	if idx.opts.ROnly {
		Logger.Trace(ErrWrite)
		return ErrWrite
	}

	for _, f := range fields {
		if f == "" {
			Logger.Trace(ErrNoWild)
			return ErrNoWild
		}
	}

	_, err = idx.One(fields)
	if err != ErrNoItem {
		Logger.Trace(ErrExists)
		return ErrExists
	}

	nd := &node{
		Item:     &Item{Fields: fields, Value: value},
		children: make(map[string]*node),
	}

	err = idx.save(nd)
	if err != nil {
		Logger.Trace(err)
		return err
	}

	// index item should be saved before adding it to the in memory index
	// otherwise index may miss some items when the server restarts
	err = idx.add(nd)
	if err != nil {
		Logger.Trace(err)
		return err
	}

	atomic.AddInt64(&idx.metrics.PutOps, 1)
	return nil
}

func (idx *index) One(fields []string) (item *Item, err error) {
	node := idx.rootNode

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

	atomic.AddInt64(&idx.metrics.OneOps, 1)
	return node.Item, nil
}

func (idx *index) Get(fields []string) (items []*Item, err error) {
	needsFilter := false

	root := idx.rootNode
	nfields := len(fields)
	var ok bool

	for i, v := range fields {
		if v == "" {
			// check whether we have any non-empty fields below
			for j := nfields - 1; j >= i; j-- {
				if fields[j] != "" {
					needsFilter = true
				}
			}

			break
		}

		if root.children == nil {
			err = idx.loadBranch(root)
			if err != nil {
				Logger.Trace(err)
				return nil, err
			}
		}

		if root, ok = root.children[v]; !ok {
			items = make([]*Item, 0)
			atomic.AddInt64(&idx.metrics.GetOps, 1)
			return items, nil
		}
	}

	items, err = idx.find(root)
	if err != nil {
		Logger.Trace(err)
		return nil, err
	}

	if !needsFilter {
		atomic.AddInt64(&idx.metrics.GetOps, 1)
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

	atomic.AddInt64(&idx.metrics.GetOps, 1)
	return filtered, nil
}

func (idx *index) Metrics() (m *Metrics) {
	metrics := *idx.metrics
	metrics.MMapSize = idx.mmapFile.Size()
	metrics.DataSize = idx.dataSize
	atomic.StoreInt64(&idx.metrics.PutOps, 0)
	atomic.StoreInt64(&idx.metrics.GetOps, 0)
	atomic.StoreInt64(&idx.metrics.OneOps, 0)
	return &metrics
}

func (idx *index) Close() (err error) {
	if idx.opts.ROnly {
		return nil
	}

	idx.addMutex.Lock()
	defer idx.addMutex.Unlock()

	err = idx.mmapFile.Close()
	if err != nil {
		Logger.Trace(err)
		return err
	}

	return nil
}

// find recursively finds and collects all nodes inside a sub-tree
func (idx *index) find(root *node) (items []*Item, err error) {
	items = make([]*Item, 0)

	if root.Value != NoValue {
		items = append(items, root.Item)
	}

	if root.children == nil {
		err = idx.loadBranch(root)
		if err != nil {
			Logger.Trace(err)
			return nil, err
		}
	}

	for _, nd := range root.children {
		res, err := idx.find(nd)
		if err != nil {
			Logger.Trace(err)
			return nil, err
		}

		items = append(items, res...)
	}

	return items, nil
}

// add adds a new node to the tree.
// intermediate nodes will be created if not available.
// If a node already exists, its value will be updated.
// This can happen when an intermediate node is set after setting
// one of its child nodes are set.
func (idx *index) add(nd *node) (err error) {
	// make sure the branch is loaded
	firstField := nd.Fields[0]
	firstNode, ok := idx.rootNode.children[firstField]
	if ok && firstNode.children == nil {
		err = idx.loadBranch(firstNode)
		if err != nil {
			Logger.Trace(err)
			return err
		}
	}

	idx.addMutex.Lock()
	defer idx.addMutex.Unlock()

	// start from the root
	root := idx.rootNode
	count := len(nd.Fields)
	mfields := nd.Fields[:count-1]

	// traverse through the tree by node fields
	// creates missing nodes upto the leaf node
	for i, field := range mfields {
		newRoot, ok := root.children[field]

		if !ok {
			// fields upto this node of the tree
			newRootFields := nd.Fields[0 : i+1]
			newRoot = &node{
				Item:     &Item{Fields: newRootFields, Value: NoValue},
				children: make(map[string]*node),
			}

			atomic.AddInt64(&idx.metrics.Nodes, 1)
			root.children[field] = newRoot
		}

		root = newRoot
	}

	// add leaf node at the end if does not exist
	// if a node already exists, update its value
	field := nd.Fields[count-1]
	leaf, ok := root.children[field]
	if ok {
		leaf.Item.Value = nd.Item.Value
	} else {
		atomic.AddInt64(&idx.metrics.Nodes, 1)
		root.children[field] = nd
	}

	atomic.AddInt64(&idx.metrics.Items, 1)
	return nil
}

// save method serializes and saves the node to disk
// format: [size int64 | payload []byte]
func (idx *index) save(nd *node) (err error) {
	itemBytes, err := proto.Marshal(nd.Item)
	if err != nil {
		Logger.Trace(err)
		return err
	}

	payloadSize := int64(len(itemBytes)) + ItemHeaderSize

	// Force allocation if we don't have enough space to save the item.
	// If allocation fails, the function will return an error to the user.
	if idx.mmapFile.Size()-idx.dataSize-payloadSize < 0 {
		idx.allocMutex.Lock()

		if idx.mmapFile.Size()-idx.dataSize-payloadSize < 0 {
			err = idx.allocate()
			if err != nil {
				idx.allocMutex.Unlock()
				Logger.Trace(err)
				return err
			}
		}

		idx.allocMutex.Unlock()
	}

	// Run pre-allocation in the background when we reach a threshold.
	// Check first in order to avoid running unnecessary goroutines.
	if !idx.allocating &&
		idx.mmapFile.Size()-idx.dataSize-payloadSize < PreallocThresh {
		idx.allocating = true
		go idx.preallocateIfNeeded()
	}

	idx.addMutex.Lock()
	defer idx.addMutex.Unlock()

	idx.dataSize += int64(payloadSize)
	itemSize := uint32(len(itemBytes))
	err = binary.Write(idx.mmapFile, binary.LittleEndian, itemSize)
	if err != nil {
		Logger.Trace(err)
		return err
	}

	n, err := idx.mmapFile.Write(itemBytes)
	if err != nil {
		Logger.Trace(err)
		return err
	} else if uint32(n) != itemSize {
		Logger.Trace(ErrWrite)
		return ErrWrite
	}

	return nil
}

// load loads nodes from the disk and builds the index in memory
func (idx *index) load() (err error) {
	buffer := idx.mmapFile
	buffrSize := buffer.Size()
	buffer.Reset()

	var dataBuff []byte

	for {
		var itemSize uint32

		err = binary.Read(buffer, binary.LittleEndian, &itemSize)
		if err != nil && err != io.EOF {
			Logger.Trace(err)
			return err
		} else if err == io.EOF || itemSize == 0 {
			// io.EOF file will occur when we're read exactly up to file end.
			// This is a very rare incident because file is preallocated.
			// As we always preallocate with zeroes, itemSize will be zero.
			break
		} else if itemSize > uint32(buffrSize-idx.dataSize) {
			// If we came to this point in this if-else ladder it means that file
			// contains an itemSize but does not have enough bytes left.
			Logger.Trace(ErrLoad)
			return ErrLoad
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
			Logger.Trace(ErrLoad)
			return ErrLoad
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

		err = idx.add(nd)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		idx.dataSize += ItemHeaderSize + int64(itemSize)
	}

	return nil
}

func (idx *index) saveSnapshot() (err error) {
	rfpath := idx.opts.Path + ".snap-root"
	dfpath := idx.opts.Path + ".snap-data"
	rfpathTmp := idx.opts.Path + ".snap-root.part"
	dfpathTmp := idx.opts.Path + ".snap-data.part"

	err = os.Remove(rfpathTmp)
	if err != nil && !os.IsNotExist(err) {
		Logger.Trace(err)
		return err
	}

	err = os.Remove(dfpathTmp)
	if err != nil && !os.IsNotExist(err) {
		Logger.Trace(err)
		return err
	}

	rfile, err := mmap.New(&mmap.Options{Path: rfpathTmp})
	if err != nil {
		Logger.Trace(err)
		return err
	}

	dfile, err := mmap.New(&mmap.Options{Path: dfpathTmp})
	if err != nil {
		Logger.Trace(err)
		return err
	}

	for _, root := range idx.rootNode.children {
		startOffset := uint32(dfile.Size())
		items, err := idx.find(root)
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
			err = binary.Write(dfile, binary.LittleEndian, itemSize)
			if err != nil {
				Logger.Trace(err)
				return err
			}

			n, err := dfile.Write(itemBytes)
			if err != nil {
				Logger.Trace(err)
				return err
			} else if uint32(n) != itemSize {
				Logger.Trace(ErrWrite)
				return ErrWrite
			}
		}

		endOffset := uint32(dfile.Size())

		item := root.Item
		itemBytes, err := proto.Marshal(item)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		itemSize := uint32(len(itemBytes))
		err = binary.Write(rfile, binary.LittleEndian, itemSize)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		n, err := rfile.Write(itemBytes)
		if err != nil {
			Logger.Trace(err)
			return err
		} else if uint32(n) != itemSize {
			Logger.Trace(ErrWrite)
			return ErrWrite
		}

		err = binary.Write(rfile, binary.LittleEndian, startOffset)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		err = binary.Write(rfile, binary.LittleEndian, endOffset)
		if err != nil {
			Logger.Trace(err)
			return err
		}
	}

	err = rfile.Close()
	if err != nil {
		Logger.Trace(err)
		return err
	}

	err = dfile.Close()
	if err != nil {
		Logger.Trace(err)
		return err
	}

	err = os.Rename(dfpathTmp, dfpath)
	if err != nil {
		Logger.Trace(err)
		return err
	}

	err = os.Rename(rfpathTmp, rfpath)
	if err != nil {
		Logger.Trace(err)
		return err
	}

	return nil
}

func (idx *index) loadSnapshot() (err error) {
	rfpath := idx.opts.Path + ".snap-root"
	rfile, err := os.OpenFile(rfpath, os.O_RDONLY, 0644)
	if err != nil {
		Logger.Trace(err)
		return err
	}

	finfo, err := rfile.Stat()
	if err != nil {
		Logger.Trace(err)
		return err
	}

	fileSize := finfo.Size()

	var dataBuff []byte
	var bytesRead int64
	var footerSize uint32 = ItemHeaderSize * 2

	for bytesRead < fileSize {
		// bytes available after reading item header
		bytesAvailable := uint32(fileSize - bytesRead - ItemHeaderSize)

		var itemSize uint32
		err = binary.Read(rfile, binary.LittleEndian, &itemSize)
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
			Logger.Trace(ErrLoad)
			return ErrLoad
		}

		if uint32(cap(dataBuff)) < itemSize {
			dataBuff = make([]byte, itemSize)
		}

		itemData := dataBuff[0:itemSize]
		n, err := rfile.Read(itemData)
		if err != nil {
			Logger.Trace(err)
			return err
		} else if uint32(n) != itemSize {
			Logger.Trace(ErrLoad)
			return ErrLoad
		}

		var startOffset uint32
		err = binary.Read(rfile, binary.LittleEndian, &startOffset)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		var endOffset uint32
		err = binary.Read(rfile, binary.LittleEndian, &endOffset)
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
		idx.offsets[firstField] = &offsets{start: startOffset, end: endOffset}

		err = idx.add(nd)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		bytesRead += ItemHeaderSize + int64(itemSize+footerSize)
	}

	return nil
}

func (idx *index) loadBranch(nd *node) (err error) {
	dfpath := idx.opts.Path + ".snap-data"
	dfile, err := os.OpenFile(dfpath, mmap.FileMode, mmap.FilePerm)
	if err != nil {
		Logger.Trace(err)
		return err
	}

	finfo, err := dfile.Stat()
	if err != nil {
		Logger.Trace(err)
		return err
	}

	nd.children = make(map[string]*node)
	firstField := nd.Fields[0]
	offsets := idx.offsets[firstField]

	dataSize := int64(offsets.end)
	fileSize := finfo.Size()
	if fileSize < int64(dataSize) {
		Logger.Trace(ErrLoad)
		return ErrLoad
	}

	var dataBuff []byte
	var bytesRead = int64(offsets.start)

	off, err := dfile.Seek(bytesRead, 0)
	if err != nil {
		Logger.Trace(err)
		return err
	} else if off != bytesRead {
		Logger.Trace(ErrLoad)
		return ErrLoad
	}

	for bytesRead < dataSize {
		// bytes available after reading item header
		bytesAvailable := uint32(dataSize - bytesRead - ItemHeaderSize)

		var itemSize uint32
		err = binary.Read(dfile, binary.LittleEndian, &itemSize)
		if err != nil && err != io.EOF {
			Logger.Trace(err)
			return err
		} else if err == io.EOF || itemSize == 0 {
			// io.EOF file will occur when we're read exactly up to file end.
			// This is a very rare incident because file is preallocated.
			// As we always preallocate with zeroes, itemSize will be zero.
			break
		} else if itemSize > bytesAvailable {
			// If we came to this point in this if-else ladder it means that file
			// contains an itemSize but does not have enough bytes left.
			Logger.Trace(ErrLoad)
			return ErrLoad
		}

		if uint32(cap(dataBuff)) < itemSize {
			dataBuff = make([]byte, itemSize)
		}

		itemData := dataBuff[0:itemSize]
		n, err := dfile.Read(itemData)
		if err != nil {
			Logger.Trace(err)
			return err
		} else if uint32(n) != itemSize {
			Logger.Trace(ErrLoad)
			return ErrLoad
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

		err = idx.add(nd)
		if err != nil {
			Logger.Trace(err)
			return err
		}

		bytesRead += ItemHeaderSize + int64(itemSize)
	}

	return nil
}

func (idx *index) preallocateIfNeeded() (err error) {
	// run allocation in the background when we reach a threshold
	if idx.mmapFile.Size()-idx.dataSize < PreallocThresh {
		idx.allocMutex.Lock()
		defer idx.allocMutex.Unlock()

		if idx.mmapFile.Size()-idx.dataSize < PreallocThresh {
			err = idx.allocate()
			if err != nil {
				idx.allocating = false
				Logger.Trace(err)
				return err
			}
		}
	}

	idx.allocating = false
	return nil
}

func (idx *index) allocate() (err error) {
	return idx.mmapFile.Grow(PreallocSize)
}
