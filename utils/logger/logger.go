package logger

import (
	"log"
	"os"
)

var (
	enableLog    = true
	enableDebug  = os.Getenv("DEBUG") != ""
	panicOnError = os.Getenv("PANIC") != ""

	logger = log.New(os.Stdout, "", log.LstdFlags)
)

// Log prints messages with a prefix
func Log(prefix string, logs ...interface{}) {
	printIfEnabled(enableLog, prefix, logs)
}

// Debug prints messages with a prefix if the `debug` env variable
// is set (to something other than an empty string)
func Debug(prefix string, logs ...interface{}) {
	printIfEnabled(enableDebug, prefix, logs)
}

func printIfEnabled(enabled bool, prefix string, logs []interface{}) {
	if enabled {
		logger.Printf("%s: ", prefix)
		logger.Println(logs...)
	}

	if panicOnError {
		panic("PANIC!!!")
	}
}
