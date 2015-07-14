package logger

import (
	"log"
	"os"
)

var (
	enabled = os.Getenv("debug") != ""
	logger  = log.New(os.Stdout, "", log.LstdFlags)
)

// Log prints the error message with a prefix if the `debug` env variable
// is set (to something other than an empty string of course)
func Log(prefix string, err error) {
	if enabled {
		logger.Printf("%s: %s\n", prefix, err.Error())
	}
}
