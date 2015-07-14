package cache

import "testing"

func TestNew(t *testing.T) {
	c, err := New(&Options{Size: 3})
	if err != nil {
		t.Fatal(err)
	}

	if c.Length() != 0 {
		t.Fatal("length should be 0")
	}
}

func TestNewEmpty(t *testing.T) {
	_, err := New(&Options{Size: 0})
	if err != ErrEmpty {
		t.Fatal("should return an error")
	}
}

func TestAdd(t *testing.T) {
	c, err := New(&Options{Size: 3})
	if err != nil {
		t.Fatal(err)
	}

	var key int64 = 1
	var val int64 = 100

	err = c.Add(key, val)
	if err != nil {
		t.Fatal(err)
	}

	if c.Length() != 1 {
		t.Fatal("incorrect length")
	}
}

func TestGet(t *testing.T) {
	c, err := New(&Options{Size: 3})
	if err != nil {
		t.Fatal(err)
	}

	var key int64 = 1
	var val int64 = 100

	err = c.Add(key, val)
	if err != nil {
		t.Fatal(err)
	}

	out, err := c.Get(key)
	if out.(int64) != val {
		t.Fatal("incorrect value")
	}
}

func TestDel(t *testing.T) {
	c, err := New(&Options{Size: 3})
	if err != nil {
		t.Fatal(err)
	}

	var key int64 = 1
	var val int64 = 100

	err = c.Add(key, val)
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Del(key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Get(key)
	if err != ErrMissing {
		t.Fatal("should return error")
	}
}

func BenchAddSize(b *testing.B, size int) {
	c, err := New(&Options{Size: size})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	var i int64
	var N = int64(b.N)
	var S = int64(size)

	for i = 0; i < N; i++ {
		err = c.Add(i%S, i)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAddSize10(b *testing.B)   { BenchAddSize(b, 10) }
func BenchmarkAddSize100(b *testing.B)  { BenchAddSize(b, 10) }
func BenchmarkAddSize1000(b *testing.B) { BenchAddSize(b, 10) }

func BenchGetSize(b *testing.B, size int) {
	c, err := New(&Options{Size: size})
	if err != nil {
		b.Fatal(err)
	}

	var i int64
	var N = int64(b.N)
	var S = int64(size)

	for i = 0; i < S; i++ {
		err = c.Add(i, i)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for i = 0; i < N; i++ {
		_, err = c.Get(i % S)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetSize10(b *testing.B)   { BenchGetSize(b, 10) }
func BenchmarkGetSize100(b *testing.B)  { BenchGetSize(b, 10) }
func BenchmarkGetSize1000(b *testing.B) { BenchGetSize(b, 10) }

func BenchmarkDel(b *testing.B) {
	c, err := New(&Options{Size: b.N})
	if err != nil {
		b.Fatal(err)
	}

	var i int64
	var N = int64(b.N)

	for i = 0; i < N; i++ {
		err = c.Add(i, i)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for i = 0; i < N; i++ {
		_, err = c.Del(i)
		if err != nil {
			b.Fatal(err)
		}
	}
}
