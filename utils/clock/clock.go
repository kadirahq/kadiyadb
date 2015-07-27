package clock

import (
	"time"
)

func init() {
	c = r
}

var (
	c clock
	r = &real{}
	t = &test{}
)

type clock interface {
	Now() (ts int64)
}

type real struct {
}

func (c *real) Now() (ts int64) {
	return time.Now().UnixNano()
}

type test struct {
	ts int64
}

func (c *test) Now() (ts int64) {
	return c.ts
}

// Now returns current time
func Now() (ts int64) {
	return c.Now()
}

// UseReal changes the clock to real mode
// This will be used by the application
func UseReal() {
	c = r
}

// UseTest changes the clock to test mode
// This will be used only for tests
func UseTest() {
	c = t
}

// Set changes the time for test clocks
func Set(ts int64) {
	t.ts = ts
}
