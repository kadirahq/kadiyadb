package block

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func TestDecode(t *testing.T) {
	b := bytes.NewBuffer(nil)
	binary.Write(b, binary.LittleEndian, float64(1.1))
	binary.Write(b, binary.LittleEndian, uint64(1))
	binary.Write(b, binary.LittleEndian, float64(2.2))
	binary.Write(b, binary.LittleEndian, uint64(2))

	d := b.Bytes()
	if len(d) != pointsz*2 {
		t.Fatal("2x float64 + 2x uint64 => 32 bytes")
	}

	r := decode(d)
	e := []Point{{1.1, 1}, {2.2, 2}}

	if !reflect.DeepEqual(e, r) {
		t.Fatal("wrong values")
	}
}

func BenchDecode(b *testing.B, sz int) {
	d := make([]byte, sz*pointsz)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decode(d)
	}
}

// time/op should not change!
func BenchmarkDecode1k(b *testing.B) { BenchDecode(b, 1000) }
func BenchmarkDecode1M(b *testing.B) { BenchDecode(b, 1000000) }
