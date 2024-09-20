package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
	DBType                   string
	DBHost                   string
	DBPort                   string
	DBUser                   string
	DBPassword               string
	DBName                   string
	DBSSLMode                string
	DBMaxConnections         uint64
	DBMaxIdleConnections     int
	DBMaxLifetimeConnections int
}

type MongoDB struct {
	client   *mongo.Client
	database *mongo.Database
}

func NewMongoDB(config *Config) (*MongoDB, error) {
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%s/%s", config.DBUser, config.DBPassword, config.DBHost, config.DBPort, config.DBName)
	if config.DBUser == "" || config.DBPassword == "" {
		uri = fmt.Sprintf("mongodb://%s:%s", config.DBHost, config.DBPort)
	}

	clientOptions := options.Client().ApplyURI(uri).
		SetMaxPoolSize(config.DBMaxConnections).
		SetMaxConnIdleTime(time.Duration(config.DBMaxIdleConnections) * time.Second).
		SetConnectTimeout(time.Duration(config.DBMaxLifetimeConnections) * time.Second)

	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, err
	}

	err = client.Ping(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	return &MongoDB{
		client:   client,
		database: client.Database(config.DBName),
	}, nil
}

func (mdb *MongoDB) Close() error {
	return mdb.client.Disconnect(context.Background())
}
