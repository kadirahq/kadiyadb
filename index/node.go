package index

import (
	"errors"
	"sync"
)

const (
	// Placeholder is used as a placeholder ID until a proper value can be set.
	// This ID can be seen right after adding new nodes to the index tree.
	Placeholder = -1
)

var (
	// ErrBadNode is used when node fields are invalid or empty
	ErrBadNode = errors.New("index node is not valid")

	// ErrBadTNode is used when tree node fields are invalid or empty
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
// The index tree is made by adding TNodes as children for other TNodes
type TNode struct {
	Node     *Node
	Children map[string]*TNode
	Mutex    *sync.RWMutex
}

// WrapNode wraps a Node into a TNode
func WrapNode(node *Node) (tn *TNode) {
	return &TNode{
		Node:     node,
		Children: map[string]*TNode{},
		Mutex:    &sync.RWMutex{},
	}
}

// Validate validates the tree node
func (n *TNode) Validate() (err error) {
	if n.Node == nil || n.Children == nil {
		return ErrBadTNode
	}

	// also validate wrapped index node
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

	// start from current node
	// this will change later
	node := n

	// fill-up intermediate nodes using given fields set
	// also find the parent node for target set of fields
	for _, f := range fields[:count-1] {
		node.Mutex.Lock()
		next, ok := node.Children[f]
		if !ok {
			next = WrapNode(nil)
			node.Children[f] = next
		}
		node.Mutex.Unlock()

		node = next
	}

	// lock the parent node and find the final node for given fields.
	// If it doesn't exist, create a node with placeholder recordID.
	// The placeholder value must be replaced as soon as possible.
	node.Mutex.Lock()
	leaf, ok := node.Children[last]
	if ok {
		tn = leaf
	} else {
		tn = WrapNode(&Node{Fields: fields, RecordID: Placeholder})
		node.Children[last] = tn
	}
	node.Mutex.Unlock()

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

	// Checks query fields to see whether the FindOne method can be used
	// Also checks for invalid or empty values given as index node fields
	// TODO: This test is done multiple times when this is called recursively.
	//       Avoid the recursion to solve this and improve find performance.
	//       This is an optimization task therefore the priority is low.
	findone := true
	for _, f := range fields {
		if f == "" {
			return nil, ErrBadNode
		}

		if f == "*" {
			findone = false
			break
		}
	}

	// The query does not have any wildcards therefore the FindOne
	// method can be used instead of the much slower Find method.
	if findone {
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

	// Break the first element of the query out of the query and look for it.
	// The rest of the query will be resolved recursively one field at a time.
	car := fields[0]
	cdr := fields[1:]

	// If the field is a wildcard, run the query for each value under this node
	// and merge results taken from each value. Use `cdr` as the query from now.
	if car == "*" {
		n.Mutex.RLock()
		for _, c := range n.Children {
			res, err := c.Find(cdr)
			if err != nil {
				n.Mutex.RUnlock()
				return nil, err
			}

			ns = append(ns, res...)
		}
		n.Mutex.RUnlock()

		return ns, nil
	}

	// The field is a specific value, look for it in this node.
	// Returns a nil slice if the matching item is not found.
	n.Mutex.RLock()
	c, ok := n.Children[car]
	n.Mutex.RUnlock()
	if !ok {
		return nil, nil
	}

	return c.Find(cdr)
}

// isValidFields checks whether given set of fields are valid.
// TODO define a `Fields` type and add these methods there.
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
