package logger

import (
	"log"
	"os"
)

var (
	enableLog   = true
	enabldDebug = os.Getenv("DEBUG") != ""

	logger = log.New(os.Stdout, "", log.LstdFlags)
)

// Log prints the error message with a prefix if the `debug` env variable
// is set (to something other than an empty string of course)
func Log(prefix string, logs ...interface{}) {
	if enableLog {
		logger.Printf("%s: ", prefix)
		logger.Println(logs...)
	}
}

// Debug prints the error message with a prefix if the `debug` env variable
// is set (to something other than an empty string of course)
func Debug(prefix string, logs ...interface{}) {
	if enabldDebug {
		logger.Printf("%s: ", prefix)
		logger.Println(logs...)
	}
}
