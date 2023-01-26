package util

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	log2 "log"
	"os"
	"strings"
)

type Logger struct {
	Writer io.Writer
	Logger zerolog.Logger
	Level  zerolog.Level
}

type BadgerLogger struct {
	Writer io.Writer
	Logger zerolog.Logger
	Level  zerolog.Level
}

func (l *BadgerLogger) Errorf(s string, i ...interface{}) {
	l.Logger.Error().Msgf(s, i)
}

func (l *BadgerLogger) Warningf(s string, i ...interface{}) {
	l.Logger.Warn().Msgf(s, i)
}

func (l *BadgerLogger) Infof(s string, i ...interface{}) {
	l.Logger.Info().Msgf(s, i)
}

func (l *BadgerLogger) Debugf(s string, i ...interface{}) {
	l.Logger.Debug().Msgf(s, i)
}

var Logging = createLogger()

func (l *Logger) toStandardLogger() *log2.Logger {
	logger := log2.New(l.Writer, "", 0)
	return logger
}

func createLogger() *Logger {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006/01/02 15:04:05"}
	output.FormatLevel = func(i interface{}) string {
		return strings.ToUpper(fmt.Sprintf("[%-4s]", i))
	}
	output.FormatFieldName = func(i interface{}) string {
		return fmt.Sprintf("%s:", i)
	}
	output.FormatFieldValue = func(i interface{}) string {
		return strings.ToUpper(fmt.Sprintf("%s", i))
	}
	l := Logger{Writer: output}
	l.Level = zerolog.DebugLevel
	l.Logger = zerolog.New(l.Writer).With().Timestamp().Logger().Level(l.Level)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	log.Logger = l.Logger
	return &l
}

func CreateBadgerLogger() *log2.Logger {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006/01/02 15:04:05"}
	output.FormatLevel = func(i interface{}) string {
		return strings.ToUpper(fmt.Sprintf("[%-4s]", i))
	}
	newLogger := NewStdLoggerWithOutput(output)
	return newLogger
}

func NewRaftLogger(output io.Writer) *zerolog.Logger {
	newLogger := Logging.Logger.With().Logger().Output(output)
	return &newLogger
}

func NewStdLoggerWithOutput(output io.Writer) *log2.Logger {
	newLogger := Logging.Logger.With().Logger().Output(output)
	stdLogger := log2.Logger{}
	stdLogger.SetFlags(0)
	stdLogger.SetOutput(newLogger)
	return &stdLogger
}
