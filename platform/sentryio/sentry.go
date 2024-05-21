package sentryio

import (
	"github.com/getsentry/sentry-go"
)

type Sentry struct {
	Dns              string
	TracesSampleRate float64
}

var SentryNotification = make(chan ErrMessage)

func (s *Sentry) InitializeSentry() error {
	err := sentry.Init(sentry.ClientOptions{
		Dsn:              s.Dns,
		TracesSampleRate: s.TracesSampleRate,
	})

	if err != nil {
		return err
	}

	return nil
}

// CaptureException captures an exception in Sentry.
func (s *Sentry) CaptureException(errMsg ErrMessage) {
	go func() {
		SentryNotification <- errMsg
	}()

	// sentry.CaptureException(err)
	// sentry.Flush(2 * time.Second) // Ensure events are sent before exiting
}
