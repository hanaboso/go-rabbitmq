package rabbitmq

// The logging interface is forwarding this blog post.
// https://dave.cheney.net/2015/11/05/lets-talk-about-logging

import (
	"fmt"
	"io/ioutil"
	"log"
)

// LoggingLevel is type for logging level constants
type LoggingLevel int

// Info is for info messages. Doesn't contain errors.
// Things that developers care about when they are developing or debugging software.
//
// Debug is only for debugging.
// Things that users care about when using your software.
const (
	Info LoggingLevel = iota
	Debug
)

// Logger is smallest possible interface for logging.
type Logger interface {
	Print(...interface{})
}

type logger struct {
	Logger
	level LoggingLevel
}

// Info prints message.
func (l *logger) Info(v ...interface{}) {
	l.Logger.Print(v)
}

// Infof prints formatted message.
func (l *logger) Infof(format string, v ...interface{}) {
	l.Logger.Print(fmt.Sprintf(format, v...))
}

// Debug prints message if logger has debug level.
func (l *logger) Debug(v ...interface{}) {
	if l.level < Debug {
		return
	}
	l.Logger.Print(v...)
}

// Debugf prints formatted message if logger has debug level.
func (l *logger) Debugf(format string, v ...interface{}) {
	if l.level < Debug {
		return
	}
	l.Logger.Print(fmt.Sprintf(format, v...))
}

// DeafLogger returns logger that uses ioutil.Discard as output
func DeafLogger() Logger {
	return log.New(ioutil.Discard, "", 0)
}
