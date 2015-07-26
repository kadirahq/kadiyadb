package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/kadirahq/kadiradb-core/utils/logger"
	"github.com/kadirahq/kadiradb-core/utils/mmap"
)

const (
	// LoggerPrefix will be used to prefix debug logs
	LoggerPrefix = "INDEX"

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

	// ErrNoWild is returned when user provides wildcard fields.
	// Occurs when requesting a specific index entry using One method.
	// Also occurs when user tries to Put an index entry with wildcards.
	ErrNoWild = errors.New("wildcards are not allowed in One requests")

	// ErrNoItem is returned when the requested element is not available
	// Only happens when requesting a specific index entry using One method.
	ErrNoItem = errors.New("requested item is not available in the index")

	// NoValue is stored when there's no value
	// It has the maximum possible value for uint32
	NoValue = ^uint32(0)
)

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

	// Close cleans up stuff, releases resources and closes the index.
	Close() (err error)
}

type node struct {
	*Item                     // values
	children map[string]*node // children nodes
}

// Options has parameters required for creating an `Index`
type Options struct {
	Path  string // path to index file
	ROnly bool   // the index is loaded only for reading
}

type index struct {
	opts       *Options      // options
	rootNode   *node         // tree root node
	mmapFile   *mmap.Map     // memory map of the file used to store the tree
	dataSize   int64         // number of bytes used in the memory map
	addMutex   *sync.Mutex   // mutex used to lock when new items are added
	allocMutex *sync.Mutex   // mutex used to lock when allocating space
	allocating bool          // indicates a pre-alloc is in progress
	buffer     *bytes.Buffer // reusable buffer to write new tree nodes
}

// New function creates an new `Index` with given `Options`
// It also loads tree nodes from the disk and builds the tree in memory.
// Finally space is allocated in disk if necessary to store mote nodes.
func New(options *Options) (_idx Index, err error) {
	mfile, err := mmap.New(&mmap.Options{Path: options.Path})
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return nil, err
	}

	err = mfile.Lock()
	if err != nil {
		logger.Log(LoggerPrefix, err)
	}

	rootNode := &node{
		Item:     &Item{},
		children: make(map[string]*node),
	}

	idx := &index{
		opts:       options,
		rootNode:   rootNode,
		mmapFile:   mfile,
		dataSize:   0, // value is set later
		addMutex:   &sync.Mutex{},
		allocMutex: &sync.Mutex{},
		buffer:     bytes.NewBuffer(nil),
	}

	if err := idx.load(); err != nil {
		mfile.Close()
		return nil, err
	}

	if options.ROnly {
		err = mfile.Close()
		if err != nil {
			logger.Log(LoggerPrefix, err)
		}
	} else {
		err = idx.preallocateIfNeeded()
		if err != nil {
			mfile.Close()
			return nil, err
		}
	}

	return idx, nil
}

func (idx *index) Put(fields []string, value uint32) (err error) {
	for _, f := range fields {
		if f == "" {
			return ErrNoWild
		}
	}

	nd := &node{
		Item:     &Item{Fields: fields, Value: value},
		children: make(map[string]*node),
	}

	err = idx.save(nd)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	// index item should be saved before adding it to the in memory index
	// otherwise index may miss some items when the server restarts
	err = idx.add(nd)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}

func (idx *index) One(fields []string) (item *Item, err error) {
	node := idx.rootNode

	var ok bool
	for _, v := range fields {
		if v == "" {
			return nil, ErrNoWild
		}

		if node, ok = node.children[v]; !ok {
			return nil, ErrNoItem
		}
	}

	if node.Item.Value == NoValue {
		return nil, ErrNoItem
	}

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

		if root, ok = root.children[v]; !ok {
			items = make([]*Item, 0)
			return items, nil
		}
	}

	items = idx.find(root, fields)
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

func (idx *index) Close() (err error) {
	if idx.opts.ROnly {
		return nil
	}

	idx.addMutex.Lock()
	defer idx.addMutex.Unlock()

	err = idx.mmapFile.Close()
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	return nil
}

// find recursively finds and collects all nodes inside a sub-tree
func (idx *index) find(root *node, fields []string) (items []*Item) {
	items = make([]*Item, 0)

	if root.Value != NoValue {
		items = append(items, root.Item)
	}

	for _, nd := range root.children {
		res := idx.find(nd, fields)
		items = append(items, res...)
	}

	return items
}

// add adds a new node to the tree.
// intermediate nodes will be created if not available.
// If a node already exists, its value will be updated.
// This can happen when an intermediate node is set after setting
// one of its child nodes are set.
func (idx *index) add(nd *node) (err error) {
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
		root.children[field] = nd
	}

	return nil
}

// save method serializes and saves the node to disk
// format: [size int64 | payload []byte]
func (idx *index) save(nd *node) (err error) {
	defer idx.buffer.Reset()

	itemBytes, err := proto.Marshal(nd.Item)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	itemSize := int64(len(itemBytes))
	err = binary.Write(idx.buffer, binary.LittleEndian, itemSize)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	n, err := idx.buffer.Write(itemBytes)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	} else if int64(n) != itemSize {
		logger.Log(LoggerPrefix, ErrWrite)
		return ErrWrite
	}

	payload := idx.buffer.Bytes()
	payloadSize := int64(len(payload))

	// force allocation if we don't have enough space to save the item
	// if allocation fails, the function will return an error to the user
	if idx.mmapFile.Size()-idx.dataSize-payloadSize < 0 {
		idx.allocMutex.Lock()

		if idx.mmapFile.Size()-idx.dataSize-payloadSize < 0 {
			err = idx.allocate()
			if err != nil {
				idx.allocMutex.Unlock()
				logger.Log(LoggerPrefix, err)
				return err
			}
		}

		idx.allocMutex.Unlock()
	}

	// run pre-allocation in the background when we reach a threshold
	// check in order to avoid running unnecessary goroutines
	if !idx.allocating && idx.mmapFile.Size()-idx.dataSize < PreallocThresh {
		idx.allocating = true
		go idx.preallocateIfNeeded()
	}

	idx.addMutex.Lock()
	offset := idx.dataSize
	idx.dataSize += int64(payloadSize)
	idx.addMutex.Unlock()

	n, err = idx.mmapFile.WriteAt(payload, offset)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	} else if int64(n) != payloadSize {
		logger.Log(LoggerPrefix, ErrWrite)
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
		var itemSize int64

		err = binary.Read(buffer, binary.LittleEndian, &itemSize)
		if err != nil && err != io.EOF {
			logger.Log(LoggerPrefix, err)
			return err
		} else if err == io.EOF || itemSize == 0 {
			// io.EOF file will occur when we're read exactly up to file end.
			// This is a very rare incident because file is preallocated.
			// As we always preallocate with zeroes, itemSize will be zero.
			break
		} else if itemSize >= buffrSize-idx.dataSize {
			// If we came to this point in this if-else ladder it means that file
			// contains an itemSize but does not have enough bytes left.
			logger.Log(LoggerPrefix, ErrLoad)
			return ErrLoad
		}

		if int64(cap(dataBuff)) < itemSize {
			dataBuff = make([]byte, itemSize)
		}

		itemData := dataBuff[0:itemSize]
		n, err := buffer.Read(itemData)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		} else if int64(n) != itemSize {
			logger.Log(LoggerPrefix, ErrLoad)
			return ErrLoad
		}

		item := &Item{}
		err = proto.Unmarshal(itemData, item)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}

		err = idx.Put(item.Fields, item.Value)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}

		idx.dataSize += ItemHeaderSize + itemSize
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
				logger.Log(LoggerPrefix, err)
				idx.allocating = false
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
