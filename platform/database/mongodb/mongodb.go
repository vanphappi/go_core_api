package mongodb

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
)

type MongoDB struct {
	DBHost string
	DBPort string
	DBName string
}

var client *mongo.Client

func NewContext() (context.Context, context.CancelFunc) {
	// return context.WithTimeout(context.Background(), 10*time.Second)
	return context.TODO(), nil
}

func Shutdown() {
	ctx, _ := NewContext()
	if client != nil {
		err := client.Disconnect(ctx)
		if err != nil {
			log.Fatalf("Cannot disconnect MongoDB: %v", err)
		}
	}
	log.Println("[MONGO_DB] disconnect successfully")
}
