// automatically generated, do not modify

package metadata

import (
	flatbuffers "github.com/kadirahq/flatbuffers/go"
)
type Metadata struct {
	_tab flatbuffers.Table
}

func GetRootAsMetadata(buf []byte, offset flatbuffers.UOffsetT) *Metadata {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Metadata{}
	x.Init(buf, n + offset)
	return x
}

func (rcv *Metadata) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Metadata) Duration() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Metadata) SetDuration(n int64) {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		rcv._tab.SetInt64(o + rcv._tab.Pos, n)
	}
}

func (rcv *Metadata) Retention() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Metadata) SetRetention(n int64) {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		rcv._tab.SetInt64(o + rcv._tab.Pos, n)
	}
}

func (rcv *Metadata) Resolution() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Metadata) SetResolution(n int64) {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		rcv._tab.SetInt64(o + rcv._tab.Pos, n)
	}
}

func (rcv *Metadata) PayloadSize() uint32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.GetUint32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Metadata) SetPayloadSize(n uint32) {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		rcv._tab.SetUint32(o + rcv._tab.Pos, n)
	}
}

func (rcv *Metadata) SegmentSize() uint32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.GetUint32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Metadata) SetSegmentSize(n uint32) {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		rcv._tab.SetUint32(o + rcv._tab.Pos, n)
	}
}

func (rcv *Metadata) MaxROEpochs() uint32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		return rcv._tab.GetUint32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Metadata) SetMaxROEpochs(n uint32) {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		rcv._tab.SetUint32(o + rcv._tab.Pos, n)
	}
}

func (rcv *Metadata) MaxRWEpochs() uint32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		return rcv._tab.GetUint32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Metadata) SetMaxRWEpochs(n uint32) {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		rcv._tab.SetUint32(o + rcv._tab.Pos, n)
	}
}

func MetadataStart(builder *flatbuffers.Builder) { builder.StartObject(7) }
func MetadataAddDuration(builder *flatbuffers.Builder, duration int64) { builder.PrependInt64Slot(0, duration, 0) }
func MetadataAddRetention(builder *flatbuffers.Builder, retention int64) { builder.PrependInt64Slot(1, retention, 0) }
func MetadataAddResolution(builder *flatbuffers.Builder, resolution int64) { builder.PrependInt64Slot(2, resolution, 0) }
func MetadataAddPayloadSize(builder *flatbuffers.Builder, payloadSize uint32) { builder.PrependUint32Slot(3, payloadSize, 0) }
func MetadataAddSegmentSize(builder *flatbuffers.Builder, segmentSize uint32) { builder.PrependUint32Slot(4, segmentSize, 0) }
func MetadataAddMaxROEpochs(builder *flatbuffers.Builder, maxROEpochs uint32) { builder.PrependUint32Slot(5, maxROEpochs, 0) }
func MetadataAddMaxRWEpochs(builder *flatbuffers.Builder, maxRWEpochs uint32) { builder.PrependUint32Slot(6, maxRWEpochs, 0) }
func MetadataEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT { return builder.EndObject() }