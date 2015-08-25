package kadiyadb

import (
	"os"
	"testing"
)

var (
	MDPath = "/tmp/test-md"
)

func TestNewMetadata(t *testing.T) {
	if err := os.RemoveAll(MDPath); err != nil {
		t.Fatal(err)
	}

	md, err := NewMetadata(MDPath, 1, 2, 3, 4, 5, 6, 7)
	if err != nil {
		t.Fatal(err)
	}

	if got := md.Duration(); got != 1 {
		t.Fatal("incorrect values", got)
	}

	if got := md.Retention(); got != 2 {
		t.Fatal("incorrect values", got)
	}

	if got := md.Resolution(); got != 3 {
		t.Fatal("incorrect values", got)
	}

	if got := md.PayloadSize(); got != 4 {
		t.Fatal("incorrect values", got)
	}

	if got := md.SegmentSize(); got != 5 {
		t.Fatal("incorrect values", got)
	}

	if got := md.MaxROEpochs(); got != 6 {
		t.Fatal("incorrect values", got)
	}

	if got := md.MaxRWEpochs(); got != 7 {
		t.Fatal("incorrect values", got)
	}

	err = md.Close()
	if err != nil {
		t.Fatal(err)
	}

	md, err = NewMetadata(MDPath, 0, 0, 0, 0, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	if got := md.Duration(); got != 1 {
		t.Fatal("incorrect values", got)
	}

	if got := md.Retention(); got != 2 {
		t.Fatal("incorrect values", got)
	}

	if got := md.Resolution(); got != 3 {
		t.Fatal("incorrect values", got)
	}

	if got := md.PayloadSize(); got != 4 {
		t.Fatal("incorrect values", got)
	}

	if got := md.SegmentSize(); got != 5 {
		t.Fatal("incorrect values", got)
	}

	if got := md.MaxROEpochs(); got != 6 {
		t.Fatal("incorrect values", got)
	}

	if got := md.MaxRWEpochs(); got != 7 {
		t.Fatal("incorrect values", got)
	}

	md.SetDuration(10)
	md.SetRetention(20)
	md.SetResolution(30)
	md.SetPayloadSize(40)
	md.SetSegmentSize(50)
	md.SetMaxROEpochs(60)
	md.SetMaxRWEpochs(70)

	if got := md.Duration(); got != 10 {
		t.Fatal("incorrect values", got)
	}

	if got := md.Retention(); got != 20 {
		t.Fatal("incorrect values", got)
	}

	if got := md.Resolution(); got != 30 {
		t.Fatal("incorrect values", got)
	}

	if got := md.PayloadSize(); got != 40 {
		t.Fatal("incorrect values", got)
	}

	if got := md.SegmentSize(); got != 50 {
		t.Fatal("incorrect values", got)
	}

	if got := md.MaxROEpochs(); got != 60 {
		t.Fatal("incorrect values", got)
	}

	if got := md.MaxRWEpochs(); got != 70 {
		t.Fatal("incorrect values", got)
	}

	err = md.Close()
	if err != nil {
		t.Fatal(err)
	}

	md, err = ReadMetadata(MDPath)
	if err != nil {
		t.Fatal(err)
	}

	if got := md.Duration(); got != 10 {
		t.Fatal("incorrect values", got)
	}

	if got := md.Retention(); got != 20 {
		t.Fatal("incorrect values", got)
	}

	if got := md.Resolution(); got != 30 {
		t.Fatal("incorrect values", got)
	}

	if got := md.PayloadSize(); got != 40 {
		t.Fatal("incorrect values", got)
	}

	if got := md.SegmentSize(); got != 50 {
		t.Fatal("incorrect values", got)
	}

	if got := md.MaxROEpochs(); got != 60 {
		t.Fatal("incorrect values", got)
	}

	if got := md.MaxRWEpochs(); got != 70 {
		t.Fatal("incorrect values", got)
	}

	md.SetDuration(100)
	md.SetRetention(200)
	md.SetResolution(300)
	md.SetPayloadSize(400)
	md.SetSegmentSize(500)
	md.SetMaxROEpochs(600)
	md.SetMaxRWEpochs(700)

	err = md.Close()
	if err != nil {
		t.Fatal(err)
	}

	md, err = ReadMetadata(MDPath)
	if err != nil {
		t.Fatal(err)
	}

	if got := md.Duration(); got != 10 {
		t.Fatal("incorrect values", got)
	}

	if got := md.Retention(); got != 20 {
		t.Fatal("incorrect values", got)
	}

	if got := md.Resolution(); got != 30 {
		t.Fatal("incorrect values", got)
	}

	if got := md.PayloadSize(); got != 40 {
		t.Fatal("incorrect values", got)
	}

	if got := md.SegmentSize(); got != 50 {
		t.Fatal("incorrect values", got)
	}

	if got := md.MaxROEpochs(); got != 60 {
		t.Fatal("incorrect values", got)
	}

	if got := md.MaxRWEpochs(); got != 70 {
		t.Fatal("incorrect values", got)
	}

	err = md.Close()
	if err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(MDPath); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkMetadataWrite(b *testing.B) {
	if err := os.RemoveAll(MDPath); err != nil {
		b.Fatal(err)
	}

	md, err := NewMetadata(MDPath, 1, 2, 3, 4, 5, 6, 7)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.SetParallelism(1000)
	b.RunParallel(func(pb *testing.PB) {
		var i int64
		for i = 0; pb.Next(); i++ {
			md.SetDuration(i)
		}
	})

	err = md.Close()
	if err != nil {
		b.Fatal(err)
	}

	if err := os.RemoveAll(MDPath); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkMetadataWriteAndSync(b *testing.B) {
	if err := os.RemoveAll(MDPath); err != nil {
		b.Fatal(err)
	}

	md, err := NewMetadata(MDPath, 1, 2, 3, 4, 5, 6, 7)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.SetParallelism(1000)
	b.RunParallel(func(pb *testing.PB) {
		var i int64
		for i = 0; pb.Next(); i++ {
			md.SetDuration(i)
			md.Sync()
		}
	})

	err = md.Close()
	if err != nil {
		b.Fatal(err)
	}

	if err := os.RemoveAll(MDPath); err != nil {
		b.Fatal(err)
	}
}
