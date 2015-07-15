// Code generated by protoc-gen-go.
// source: protocol.proto
// DO NOT EDIT!

/*
Package block is a generated protocol buffer package.

It is generated from these files:
	protocol.proto

It has these top-level messages:
	Metadata
*/
package block

import proto "github.com/golang/protobuf/proto"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal

type Metadata struct {
	// number of records per segment
	SegmentLength int64 `protobuf:"varint,1,opt,name=segmentLength" json:"segmentLength,omitempty"`
	// number of segment files in use
	SegmentCount int64 `protobuf:"varint,2,opt,name=segmentCount" json:"segmentCount,omitempty"`
	// total number of records in use
	// calculated across all segments
	RecordCount int64 `protobuf:"varint,3,opt,name=recordCount" json:"recordCount,omitempty"`
}

func (m *Metadata) Reset()         { *m = Metadata{} }
func (m *Metadata) String() string { return proto.CompactTextString(m) }
func (*Metadata) ProtoMessage()    {}
