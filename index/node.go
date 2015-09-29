package index

import (
	"errors"
	"sync"
)

var (
	// ErrBadNode is used when node fields are not valid
	ErrBadNode = errors.New("index node is not valid")
)

// Validate validates the node
func (n *Node) Validate() (err error) {
	if n.Fields == nil {
		return ErrBadNode
	}

	for _, f := range n.Fields {
		if f == "" || f == "*" {
			return ErrBadNode
		}
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
		return ErrBadNode
	}

	if err := n.Node.Validate(); err != nil {
		return err
	}

	return nil
}

// Append attachs a TNode taking this TNode as the root.
// Temporary TNode structs will be created when needed.
// This function should not fail (do not return error).
func (n *TNode) Append(m *TNode) {
	node := n
	flen := len(m.Node.Fields)
	path := m.Node.Fields[:flen-1]
	last := m.Node.Fields[flen-1]

	for _, f := range path {
		node.Mutex.Lock()
		next, ok := node.Children[f]
		if !ok {
			next = WrapNode(nil)
			node.Children[f] = next
		}
		node.Mutex.Unlock()

		node = next
	}

	node.Mutex.Lock()
	leaf, ok := node.Children[last]
	if ok {
		leaf.Node = m.Node
	} else {
		node.Children[last] = m
	}
	node.Mutex.Unlock()
}

// FindOne finds the index nodes with exact given field combination.
// `n` is nil if the no nodes exist in the index with given fields.
func (n *TNode) FindOne(fields []string) (res *Node, err error) {
	c := n

	for _, f := range fields {
		if f == "" || f == "*" {
			return nil, ErrBadNode
		}

		c.Mutex.RLock()
		next, ok := c.Children[f]
		c.Mutex.RUnlock()
		if !ok {
			return nil, nil
		}

		c = next
	}

	return c.Node, nil
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
