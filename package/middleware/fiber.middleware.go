package middleware

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

//var privateKey = encryption.ReadPrivateKey()

// FiberMiddleware provide Fiber's built-in middlewares.
// See: https://docs.gofiber.io/api/middleware
func FiberMiddleware(a *fiber.App) {
	a.Use(
		// Add CORS to each route.
		// cors.New(),
		// Add simple logger.
		logger.New(),

		// // JWT Middleware
		// jwtware.New(jwtware.Config{
		// 	SigningKey: jwtware.SigningKey{
		// 		JWTAlg: jwtware.RS256,
		// 		Key:    privateKey.Public(),
		// 	},
		// }),
	)
}
