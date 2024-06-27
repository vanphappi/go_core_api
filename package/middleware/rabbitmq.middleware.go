package middleware

import (
	"context"

	"github.com/streadway/amqp"
)

// Middleware function type
type Middleware func(next HandlerFunc) HandlerFunc

// HandlerFunc type for message handling
type HandlerFunc func(ctx context.Context, d amqp.Delivery)

// UseMiddleware applies middleware to a handler function
func UseMiddleware(handler HandlerFunc, middleware ...Middleware) HandlerFunc {
	for _, m := range middleware {
		handler = m(handler)
	}
	return handler
}
