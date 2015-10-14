package epoch

import (
	"os"
	"testing"
)

var (
	tmpdirc = "/tmp/test-cache/"
)

func setupc(t testing.TB) func() {
	if err := os.RemoveAll(tmpdirc); err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(tmpdirc, 0777); err != nil {
		t.Fatal(err)
	}

	return func() {
		if err := os.RemoveAll(tmpdirc); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNewCache(t *testing.T) {
	defer setupc(t)()

	for i := 0; i < 3; i++ {
		c := NewCache(2, 2, tmpdirc, 5)

		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOpenCache(t *testing.T) {
	defer setupc(t)()

	c := NewCache(2, 2, tmpdirc, 5)

	e, err := c.LoadRW(0)
	if err != nil {
		t.Fatal(err)
	}

	if err := e.Track(0, []string{"a"}, 1, 1); err != nil {
		t.Fatal(err)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	c = NewCache(2, 2, tmpdirc, 5)

	e, err = c.LoadRO(0)
	if err != nil {
		t.Fatal(err)
	}

	ps, _, err := e.Fetch(0, 1, []string{"a"})
	if err != nil {
		t.Fatal(err)
	}

	point := ps[0][0]
	if point.Total != 1 || point.Count != 1 {
		t.Fatal(err)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCacheLoadRO(t *testing.T) {
	defer setupc(t)()

	for i := 0; i < 3; i++ {
		c := NewCache(2, 2, tmpdirc, 5)

		for j := 0; j < 3; j++ {
			if _, err := c.LoadRO(0); err != nil {
				t.Fatal(err)
			}

			if len(c.rodata) != 1 {
				t.Fatal("wrong count")
			}
		}

		if _, err := c.LoadRO(1); err != nil {
			t.Fatal(err)
		}

		for j := 2; j < 5; j++ {
			if _, err := c.LoadRO(int64(i)); err != nil {
				t.Fatal(err)
			}

			if len(c.rodata) != 2 {
				t.Fatal("wrong count")
			}
		}

		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCacheLoadRW(t *testing.T) {
	defer setupc(t)()

	for i := 0; i < 3; i++ {
		c := NewCache(2, 2, tmpdirc, 5)

		for j := 0; j < 3; j++ {
			if _, err := c.LoadRW(0); err != nil {
				t.Fatal(err)
			}

			if len(c.rwdata) != 1 {
				t.Fatal("wrong count")
			}
		}

		if _, err := c.LoadRW(1); err != nil {
			t.Fatal(err)
		}

		for j := 2; j < 5; j++ {
			if _, err := c.LoadRW(int64(i)); err != nil {
				t.Fatal(err)
			}

			if len(c.rwdata) != 2 {
				t.Fatal("wrong count")
			}
		}

		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCacheLoadRORW(t *testing.T) {
	defer setupc(t)()

	c := NewCache(2, 2, tmpdirc, 5)

	if _, err := c.LoadRO(0); err != nil {
		t.Fatal(err)
	}

	if len(c.rodata) != 1 {
		t.Fatal("wrong count")
	}
	if len(c.rwdata) != 0 {
		t.Fatal("wrong count")
	}

	if _, err := c.LoadRW(0); err != nil {
		t.Fatal(err)
	}

	if len(c.rodata) != 0 {
		t.Fatal("wrong count")
	}
	if len(c.rwdata) != 1 {
		t.Fatal("wrong count")
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCacheLoadRWRO(t *testing.T) {
	defer setupc(t)()

	c := NewCache(2, 2, tmpdirc, 5)

	if _, err := c.LoadRW(0); err != nil {
		t.Fatal(err)
	}

	if len(c.rodata) != 0 {
		t.Fatal("wrong count")
	}
	if len(c.rwdata) != 1 {
		t.Fatal("wrong count")
	}

	if _, err := c.LoadRO(0); err != nil {
		t.Fatal(err)
	}

	if len(c.rodata) != 0 {
		t.Fatal("wrong count")
	}
	if len(c.rwdata) != 1 {
		t.Fatal("wrong count")
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncCache(t *testing.T) {
	defer setupc(t)()

	c := NewCache(2, 2, tmpdirc, 5)

	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
