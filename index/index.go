package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/meteorhacks/kadiradb-core/utils/logger"
	"github.com/meteorhacks/kadiradb-core/utils/mmap"
)

const (
	// LoggerPrefix will be used to prefix debug logs
	LoggerPrefix = "INDEX"

	// PreallocSize is the number of bytes to pre-allocate when the indes
	// file runs out of space to store new elements. Space on disk is
	// allocated and memory mapped in order to increase write performance
	// 10 MB will be preallocated when the index file runs out of space.
	PreallocSize = 1024 * 1024 * 10

	// ItemHeaderSize is the number of bytes stored used to store metadata
	// with each Item (protobuf). Currently it only contains the Item size.
	ItemHeaderSize = 8
)

var (
	// ErrWrite is returned when number of bytes doesn't match data size
	ErrWrite = errors.New("number of bytes written doesn't match data size")
	// ErrCorrupt is returned when there's an error reading data from file
	ErrCorrupt = errors.New("there's an error reading items from the file")
	// ErrWildcard is returned when user provides wildcard fields.
	// Only happens when requesting a specific index entry using One method.
	ErrWildcard = errors.New("wildcards are not allowed in One requests")
	// ErrNotFound is returned when the requested element is not available
	// Only happens when requesting a specific index entry using One method.
	ErrNotFound = errors.New("requested item is not available in the index")
)

// Index is a simple data structure to store binary data and associate it
// with a number of fields (string). Data can be stored on both leaf nodes
// and intermediate nodes.
type Index interface {
	// Put adds a new node into the tree and saves it to the disk.
	// Intermediate nodes are created in memory if not available.
	Put(fields []string, value []byte) (err error)

	// One is used to query a specific node from the tree.
	// returns ErrNotFound if the node is not available.
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
	Path string // path to index file
}

type index struct {
	opts     *Options      // options
	rootNode *node         // tree root node
	mmapFile *mmap.Map     // memory map of the file used to store the tree
	dataSize int64         // number of bytes used in the memory map
	mutex    sync.Mutex    // mutex used to lock when new items are added
	buffer   *bytes.Buffer // reusable buffer to write new tree nodes
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

	rootNode := &node{
		Item:     &Item{},
		children: make(map[string]*node),
	}

	idx := &index{
		opts:     options,
		rootNode: rootNode,
		mmapFile: mfile,
		dataSize: 0, // value is set later
		mutex:    sync.Mutex{},
		buffer:   new(bytes.Buffer),
	}

	if err := idx.load(); err != nil {
		return nil, err
	}

	return idx, nil
}

func (idx *index) Put(fields []string, value []byte) (err error) {
	nd := &node{
		Item:     &Item{Fields: fields, Value: value},
		children: make(map[string]*node),
	}

	err = idx.add(nd)
	if err != nil {
		logger.Log(LoggerPrefix, err)
		return err
	}

	err = idx.save(nd)
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
			return nil, ErrWildcard
		}

		if node, ok = node.children[v]; !ok {
			return nil, ErrNotFound
		}
	}

	if node.Item.Value == nil {
		return nil, ErrNotFound
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
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

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

	if root.Value != nil {
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
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

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
				Item:     &Item{Fields: newRootFields, Value: nil},
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
	idx.mutex.Lock()
	defer idx.mutex.Unlock()
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

	if idx.mmapFile.Size-idx.dataSize-payloadSize < 0 {
		err = idx.mmapFile.Grow(PreallocSize)
		if err != nil {
			logger.Log(LoggerPrefix, err)
			return err
		}
	}

	offset := idx.dataSize

	var i int64
	for i = 0; i < payloadSize; i++ {
		idx.mmapFile.Data[offset+i] = payload[i]
	}

	idx.dataSize += int64(payloadSize)
	return nil
}

// load loads nodes from the disk and builds the index in memory
func (idx *index) load() (err error) {
	buffer := bytes.NewReader(idx.mmapFile.Data)
	buffrSize := int64(len(idx.mmapFile.Data))

	var itemSize int64
	var dataSize int64
	var dataBuff []byte

	for {
		itemSize = 0

		err = binary.Read(buffer, binary.LittleEndian, &itemSize)
		if err != nil && err != io.EOF {
			logger.Log(LoggerPrefix, err)
			return err
		} else if err == io.EOF || itemSize == 0 {
			// io.EOF file will occur when we're read exactly up to file end.
			// This is a very rare incident because file is preallocated.
			// As we always preallocate with zeroes, itemSize will be zero.
			break
		} else if itemSize >= buffrSize-dataSize {
			// If we came to this point in this if-else ladder it means that file
			// contains an itemSize but does not have enough bytes left.
			logger.Log(LoggerPrefix, ErrCorrupt)
			return ErrCorrupt
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
			logger.Log(LoggerPrefix, ErrCorrupt)
			return ErrCorrupt
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

		dataSize += ItemHeaderSize + itemSize
	}

	return nil
}
