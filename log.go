package rabbitmq

// The logging interface is forwarding this blog post.
// https://dave.cheney.net/2015/11/05/lets-talk-about-logging

import (
	"fmt"
	"io/ioutil"
	"log"
)

type LoggingLevel int

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

func (l *logger) Info(v ...interface{}) {
	l.Logger.Print(v)
}

func (l *logger) Infof(format string, v ...interface{}) {
	l.Logger.Print(fmt.Sprintf(format, v...))
}

func (l *logger) Debug(v ...interface{}) {
	if l.level < Debug {
		return
	}
	l.Logger.Print(v...)
}

func (l *logger) Debugf(format string, v ...interface{}) {
	if l.level < Debug {
		return
	}
	l.Logger.Print(fmt.Sprintf(format, v...))
}

func DeafLogger() Logger {
	return log.New(ioutil.Discard, "", 0)
}
