package logger

import (
	"github.com/rs/zerolog"
	"io"
	"os"
	"sync"
	"time"
)

var (
	once    sync.Once
	log     zerolog.Logger
	verbose bool
)

type LevelWriter struct {
	io.Writer
	ErrorWriter io.Writer
}

func (l *LevelWriter) WriteLevel(level zerolog.Level, p []byte) (n int, err error) {
	if level > zerolog.WarnLevel {
		return l.ErrorWriter.Write(p)
	} else {
		return l.Writer.Write(p)
	}
}

func EnableVerbose() {
	verbose = true
}

func newLogger(verbose bool) zerolog.Logger {
	var level zerolog.Level
	if verbose {
		level = zerolog.DebugLevel
	} else {
		level = zerolog.InfoLevel
	}

	w := &LevelWriter{
		Writer:      zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339},
		ErrorWriter: zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339},
	}
	return zerolog.New(w).
		Level(level).
		With().
		Timestamp().
		Caller().
		Int("pid", os.Getpid()).
		Logger()
}

func Get() zerolog.Logger {
	once.Do(func() {
		log = newLogger(verbose)
	})
	return log
}
