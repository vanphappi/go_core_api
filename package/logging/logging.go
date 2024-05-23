package logging

import "log"

// Logger interface defines logging methods.
type Logger interface {
	Printf(format string, v ...interface{})
	Write(p []byte) (n int, err error)
}

// customLogger wraps the standard log.Logger.
type CustomLogger struct {
	*log.Logger
}

func (c *CustomLogger) Write(p []byte) (n int, err error) {
	return c.Logger.Writer().Write(p)
}
