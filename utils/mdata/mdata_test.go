package mdata

import (
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
)

func TestNewRO(t *testing.T) {
	fpath := "/tmp/md1"
	defer os.Remove(fpath)

	pb := &Message{Foo: "bar"}
	_, err := New(fpath, pb, true)
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewRW(t *testing.T) {
	fpath := "/tmp/md1"
	defer os.Remove(fpath)

	pb := &Message{Foo: "bar"}
	_, err := New(fpath, pb, false)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoad(t *testing.T) {
	fpath := "/tmp/md1"
	defer os.Remove(fpath)

	pb := &Message{Foo: "bar"}
	md, err := New(fpath, pb, false)
	if err != nil {
		t.Fatal(err)
	}

	err = md.Load()
	if err != nil {
		t.Fatal(err)
	}

	if pb.Foo != "bar" {
		t.Fatal("incorrect values")
	}
}

func TestSaveLoad(t *testing.T) {
	fpath := "/tmp/md1"
	defer os.Remove(fpath)

	pb := &Message{Foo: "bar"}
	md, err := New(fpath, pb, false)
	if err != nil {
		t.Fatal(err)
	}

	err = md.Save()
	if err != nil {
		t.Fatal(err)
	}

	pb.Foo = "baz"
	err = md.Load()
	if err != nil {
		t.Fatal(err)
	}

	if pb.Foo != "bar" {
		t.Fatal("incorrect values")
	}
}

func BenchmarkSave(b *testing.B) {
	fpath := "/tmp/md1"
	defer os.Remove(fpath)

	pb := &Message{Foo: "bar"}
	md, err := New(fpath, pb, false)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		md.Save()
	}
}

func BenchmarkLoad(b *testing.B) {
	fpath := "/tmp/md1"
	defer os.Remove(fpath)

	pb := &Message{Foo: "bar"}
	md, err := New(fpath, pb, false)
	if err != nil {
		b.Fatal(err)
	}

	err = md.Save()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		md.Load()
	}
}

//   Test Protocol Buffer Message
// --------------------------------

type Message struct {
	Foo string `protobuf:"bytes,1,opt,name=foo" json:"foo,omitempty"`
}

func (m *Message) Reset()         { *m = Message{} }
func (m *Message) String() string { return proto.CompactTextString(m) }
func (*Message) ProtoMessage()    {}
