package clock

import (
	"testing"
)

func TestTestClock(t *testing.T) {
	Test()
	SetTime(123)
	if Now() != 123 {
		t.Fatal("test clock should return preset value")
	}
}
