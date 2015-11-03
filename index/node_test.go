package index

import "testing"

func TestValidateNode(t *testing.T) {
	n := &Node{}
	if err := n.Validate(); err != ErrBadNode {
		t.Fatal("should return err")
	}

	n = &Node{RecordID: Placeholder}
	if err := n.Validate(); err != ErrBadNode {
		t.Fatal("should return err")
	}

	n = &Node{RecordID: 1, Fields: []string{}}
	if err := n.Validate(); err != ErrBadNode {
		t.Fatal("should return err")
	}

	n = &Node{RecordID: 1, Fields: []string{"a", ""}}
	if err := n.Validate(); err != ErrBadNode {
		t.Fatal("should return err")
	}

	n = &Node{RecordID: 1, Fields: []string{"a", "*"}}
	if err := n.Validate(); err != ErrBadNode {
		t.Fatal("should return err")
	}

	n = &Node{RecordID: 1, Fields: []string{"a"}}
	if err := n.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateTNode(t *testing.T) {
	tn := WrapNode(&Node{RecordID: 1, Fields: []string{"a"}})
	if err := tn.Validate(); err != nil {
		t.Fatal(err)
	}

	tn = WrapNode(&Node{RecordID: 1, Fields: []string{"a"}})
	tn.Node = nil
	if err := tn.Validate(); err != ErrBadTNode {
		t.Fatal("should return error")
	}

	tn = WrapNode(&Node{RecordID: 1, Fields: []string{"a"}})
	tn.Children = nil
	if err := tn.Validate(); err != ErrBadTNode {
		t.Fatal("should return error")
	}

	tn = WrapNode(&Node{RecordID: 1, Fields: []string{"a"}})
	tn.Node.RecordID = Placeholder
	if err := tn.Validate(); err != ErrBadNode {
		t.Fatal("should return error")
	}
}
