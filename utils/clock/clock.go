package clock

import (
	"time"
)

var (
	c clock = r
	r       = &realClock{}
	t       = &testClock{}
)

type clock interface {
	Now() (ts int64)
}

type realClock struct {
}

func (c *realClock) Now() (ts int64) {
	return time.Now().UnixNano()
}

type testClock struct {
	ts int64
}

func (c *testClock) Now() (ts int64) {
	return c.ts
}

// Now returns current time
func Now() (ts int64) {
	return c.Now()
}

// Real changes the clock to real mode
// This will be used by the application
func Real() {
	c = r
}

// Test changes the clock to test mode
// This will be used only for tests
func Test() {
	c = t
}

// SetTime changes the time for test clocks
func SetTime(ts int64) {
	t.ts = ts
}
