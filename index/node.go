package index

import (
	"errors"
	"math"
	"sync"
)

const (
	// Placeholder is used as a placeholder ID until a proper value can be set.
	// This ID can be seen right after adding new nodes to the index tree.
	Placeholder = math.MaxUint64
)

var (
	// ErrBadNode is used when node fields are not valid
	ErrBadNode = errors.New("index node is not valid")

	// ErrBadTNode is used when node fields are not valid
	ErrBadTNode = errors.New("index tree node is not valid")
)

// Validate validates the node
func (n *Node) Validate() (err error) {
	if !isValidFields(n.Fields) || n.RecordID == Placeholder {
		return ErrBadNode
	}

	return nil
}

// TNode is a node of the index tree the way it's used in the application
type TNode struct {
	Node     *Node
	Children map[string]*TNode
	Mutex    *sync.RWMutex
}

// WrapNode creates a TNode struct using a Node struct
func WrapNode(node *Node) (tn *TNode) {
	return &TNode{
		Node:     node,
		Children: map[string]*TNode{},
		Mutex:    &sync.RWMutex{},
	}
}

// Validate validates the node
func (n *TNode) Validate() (err error) {
	if n.Node == nil || n.Children == nil {
		return ErrBadTNode
	}

	if err := n.Node.Validate(); err != nil {
		return err
	}

	return nil
}

// Ensure ensures that a node exists with given set of fields under given node.
// Intermediate TNode structs will be created when needed to build the tree.
// NOTE: The RecordID field can be invalid on returned node. If this happens
// the field must be updated. Please use the mutex in the node when updating.
func (n *TNode) Ensure(fields []string) (tn *TNode) {
	count := len(fields)
	last := fields[count-1]
	parent := addPath(n, fields[:count-1])

	parent.Mutex.Lock()
	leaf, ok := parent.Children[last]
	if ok {
		tn = leaf
	} else {
		tn = WrapNode(&Node{Fields: fields, RecordID: Placeholder})
		parent.Children[last] = tn
	}
	parent.Mutex.Unlock()

	return tn
}

// FindOne finds the index nodes with exact given field combination.
// `n` is nil if the no nodes exist in the index with given fields.
func (n *TNode) FindOne(fields []string) (res *Node, err error) {
	c := n

	if !isValidFields(fields) {
		return nil, ErrBadNode
	}

	for _, f := range fields {
		c.Mutex.RLock()
		next, ok := c.Children[f]
		c.Mutex.RUnlock()
		if !ok {
			return nil, nil
		}

		c = next
	}

	c.Mutex.RLock()
	res = c.Node
	if res.RecordID == Placeholder {
		c.Mutex.RUnlock()
		return nil, nil
	}
	c.Mutex.RUnlock()

	return res, nil
}

// Find finds all nodes matching the field pattern under this node.
// Find runs recursively for each field until all nodes are collected.
func (n *TNode) Find(fields []string) (ns []*Node, err error) {
	if len(fields) == 0 {
		ns = []*Node{n.Node}
		return ns, nil
	}

	fast := true
	for _, f := range fields {
		if f == "" {
			return nil, ErrBadNode
		}

		if f == "*" {
			fast = false
			break
		}
	}

	if fast {
		c, err := n.FindOne(fields)
		if err != nil {
			return nil, err
		}

		if c != nil {
			ns = []*Node{c}
			return ns, nil
		}

		return nil, nil
	}

	f := fields[0]
	r := fields[1:]

	if f == "*" {
		n.Mutex.RLock()
		for _, c := range n.Children {
			res, err := c.Find(r)
			if err != nil {
				n.Mutex.RUnlock()
				return nil, err
			}

			ns = append(ns, res...)
		}
		n.Mutex.RUnlock()

		return ns, nil
	}

	n.Mutex.RLock()
	c, ok := n.Children[f]
	n.Mutex.RUnlock()
	if !ok {
		return nil, nil
	}

	return c.Find(r)
}

func addPath(n *TNode, fields []string) (node *TNode) {
	node = n

	for _, f := range fields {
		node.Mutex.Lock()
		next, ok := node.Children[f]
		if !ok {
			next = WrapNode(nil)
			node.Children[f] = next
		}
		node.Mutex.Unlock()

		node = next
	}

	return node
}

func isValidFields(fields []string) bool {
	if fields == nil || len(fields) == 0 {
		return false
	}

	for _, f := range fields {
		if f == "" || f == "*" {
			return false
		}
	}

	return true
}
