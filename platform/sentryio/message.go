package sentryio

import (
	"fmt"

	"github.com/getsentry/sentry-go"
)

type ErrMessage struct {
	EventID *sentry.EventID
	Message error
}

func (m *ErrMessage) String() string {
	return fmt.Sprintf("EventID: %s, Message: %s", *m.EventID, m.Message.Error())
}
