package logging

import (
	"io"
	"os"

	"github.com/rs/zerolog"
)

var Global zerolog.Logger

type Format int

const (
	Pretty = iota
	Structured
)

type Options struct {
	Format Format
	Level  zerolog.Level
}

var Opts = Options{Format: Pretty, Level: zerolog.InfoLevel}

type LevelWriter struct {
	io.Writer
	ErrorWriter io.Writer
}

func (l *LevelWriter) WriteLevel(level zerolog.Level, p []byte) (n int, err error) {
	w := l.Writer
	if level > zerolog.WarnLevel {
		w = l.ErrorWriter
	}
	return w.Write(p)
}

func InitLogger() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(Opts.Level)

	var stdoutWriter io.Writer
	var stderrWriter io.Writer
	if Opts.Format == Pretty {
		stdoutWriter = zerolog.ConsoleWriter{Out: os.Stdout}
		stderrWriter = zerolog.ConsoleWriter{Out: os.Stderr}
	}
	leveledWriter := &LevelWriter{Writer: stdoutWriter, ErrorWriter: stderrWriter}
	Global = zerolog.New(leveledWriter).With().Timestamp().Caller().Logger()
}
