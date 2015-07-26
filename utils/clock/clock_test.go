package clock

import (
	"testing"
)

func TestSet(t *testing.T) {
	UseTest()
	Set(123)
	if Now() != 123 {
		t.Fatal("test clock should return preset value")
	}
}
