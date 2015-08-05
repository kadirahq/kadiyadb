package logger

import (
	"log"
	"os"
)

var (
	enableDebug = os.Getenv("debug") != ""
	enableTrace = os.Getenv("trace") != ""
	enablePanic = os.Getenv("panic") != ""

	lg = log.New(os.Stdout, "", log.LstdFlags)
)

func init() {
	if enableDebug || enableTrace {
		lg.Print("LOGGER: logging debug logs")
	}

	if enableTrace {
		lg.Print("LOGGER: logging trace logs")
	}

	if enablePanic {
		lg.Print("LOGGER: panic on error")
	}
}

// Logger logs stuff
type Logger struct {
	prefix string
}

// Log prints important information.
func (l *Logger) Log(logs ...interface{}) {
	lg.Print(l.prefix + ":")
	lg.Print(logs...)
}

// Error prints error messages. Panics if PANIC env is set.
func (l *Logger) Error(logs ...interface{}) {
	lg.Print("ERROR: ")
	lg.Print(l.prefix + ":")
	lg.Print(logs...)

	if enablePanic {
		panic("LOGGER: panic")
	}
}

// Debug prints debug messages if DEBUG env or TRACE env is set.
func (l *Logger) Debug(logs ...interface{}) {
	if enableDebug || enableTrace {
		lg.Print(l.prefix + ":")
		lg.Print(logs...)
	}
}

// Trace prints verbose debug messages if TRACE env is set.
func (l *Logger) Trace(logs ...interface{}) {
	if enableTrace {
		lg.Print(l.prefix + ":")
		lg.Print(logs...)
	}
}
