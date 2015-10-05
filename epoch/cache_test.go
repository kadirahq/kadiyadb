package epoch

import (
	"os"
	"testing"
)

func TestNewCacheRW(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	for j := 0; j < 3; j++ {
		c := NewCache(2, dir, 10, false)

		_, err := c.Fetch(1000)
		if err != nil {
			t.Fatal(err)
		}

		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestNewCacheRO(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	c := NewCache(2, dir, 10, true)
	if _, err := c.Fetch(1000); err == nil {
		t.Fatal("should return error")
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	c = NewCache(2, dir, 10, false)
	if _, err := c.Fetch(1000); err != nil {
		t.Fatal(err)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	c = NewCache(2, dir, 10, true)
	if _, err := c.Fetch(1000); err != nil {
		t.Fatal(err)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestCacheFetch(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	c := NewCache(2, dir, 10, false)

	if _, ok := c.data[1000]; ok {
		t.Fatal("should not exist")
	}

	if _, err := c.Fetch(1000); err != nil {
		t.Fatal(err)
	}

	if _, ok := c.data[1000]; !ok {
		t.Fatal("should have epoch")
	}

	if _, err := c.Fetch(1000); err != nil {
		t.Fatal(err)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestCacheSize(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	c := NewCache(2, dir, 10, false)

	if len(c.data) != 0 {
		t.Fatal("wrong size")
	}

	for i := 1; i <= 5; i++ {
		if _, err := c.Fetch(int64(i) * 1000); err != nil {
			t.Fatal(err)
		}
	}

	if len(c.data) != 2 {
		t.Fatal("wrong size")
	}

	for i := 1; i <= 3; i++ {
		if _, ok := c.data[int64(i)*1000]; ok {
			t.Fatal("should not have epoch")
		}
	}

	for i := 4; i <= 5; i++ {
		if _, ok := c.data[int64(i)*1000]; !ok {
			t.Fatal("should have epoch")
		}
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestCacheExpire(t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	c := NewCache(2, dir, 10, false)

	if _, err := c.Fetch(1000); err != nil {
		t.Fatal(err)
	}

	if _, err := c.Fetch(2000); err != nil {
		t.Fatal(err)
	}

	c.Expire(2000)
	if _, ok := c.data[1000]; ok {
		t.Fatal("shouldn't have epoch")
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}
