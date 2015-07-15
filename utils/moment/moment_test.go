package moment

import (
	"testing"
)

func TParse(t *testing.T, str string, n int64) {
	ns, err := Parse(str)
	if err != nil {
		t.Fatal(err)
	} else if ns != n {
		t.Fatal("incorrect value")
	}
}

func TestParse_ns(t *testing.T)   { TParse(t, "1ns", 1) }
func TestParse_us(t *testing.T)   { TParse(t, "1us", 1e3) }
func TestParse_ms(t *testing.T)   { TParse(t, "1ms", 1e6) }
func TestParse_s(t *testing.T)    { TParse(t, "1s", 1e9) }
func TestParse_min(t *testing.T)  { TParse(t, "1min", 60*1e9) }
func TestParse_hour(t *testing.T) { TParse(t, "1hour", 60*60*1e9) }
func TestParse_day(t *testing.T)  { TParse(t, "1day", 24*60*60*1e9) }

func BParse(b *testing.B, str string) {
	for i := 0; i < b.N; i++ {
		Parse(str)
	}
}

func BenchmarkParse_ns(b *testing.B)   { BParse(b, "1ns") }
func BenchmarkParse_us(b *testing.B)   { BParse(b, "1us") }
func BenchmarkParse_ms(b *testing.B)   { BParse(b, "1ms") }
func BenchmarkParse_s(b *testing.B)    { BParse(b, "1s") }
func BenchmarkParse_min(b *testing.B)  { BParse(b, "1min") }
func BenchmarkParse_hour(b *testing.B) { BParse(b, "1hour") }
func BenchmarkParse_day(b *testing.B)  { BParse(b, "1day") }
