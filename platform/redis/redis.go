package redis

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisCluster represents a collection of Redis clients connected to multiple clusters
type RedisCluster struct {
	clusters map[string]*redis.Client
}

// NewRedisCluster initializes a new RedisCluster instance
func NewRedisCluster(clusterConfigs map[string]*redis.Options) (*RedisCluster, error) {
	clusters := make(map[string]*redis.Client)
	for name, options := range clusterConfigs {
		client := redis.NewClient(options)
		pong, err := client.Ping(context.Background()).Result()
		if err != nil {
			return nil, fmt.Errorf("error connecting to Redis cluster '%s': %v", name, err)
		}
		fmt.Printf("Connected to Redis cluster '%s': %s\n", name, pong)
		clusters[name] = client
	}
	return &RedisCluster{clusters: clusters}, nil
}

// GetClient returns the Redis client for the specified cluster
func (rc *RedisCluster) GetClient(clusterName string) (*redis.Client, error) {
	client, ok := rc.clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("redis cluster '%s' not found", clusterName)
	}
	return client, nil
}

// Close closes connections to all Redis clusters
func (rc *RedisCluster) Close() error {
	for name, client := range rc.clusters {
		err := client.Close()
		if err != nil {
			return fmt.Errorf("error closing Redis cluster '%s' connection: %v", name, err)
		}
		fmt.Printf("Closed connection to Redis cluster '%s'\n", name)
	}
	return nil
}

// RedisClient is a struct representing the Redis client
type RedisClient struct {
	client *redis.Client
}

// NewRedisClient initializes a new RedisClient instance
func NewRedisClient(addr, password string, db int) (*RedisClient, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	// Ping the Redis server to ensure the connection is established
	pong, err := rdb.Ping(context.Background()).Result()
	if err != nil {
		return nil, fmt.Errorf("error connecting to Redis: %v", err)
	}
	fmt.Println("Connected to Redis:", pong)

	return &RedisClient{client: rdb}, nil
}

// Set sets the value for the given key
func (r *RedisClient) Set(ctx context.Context, key string, value interface{}) error {
	err := r.client.Set(ctx, key, value, 0).Err()
	if err != nil {
		return fmt.Errorf("error setting value: %v", err)
	}
	return nil
}

// Get gets the value for the given key
func (r *RedisClient) Get(ctx context.Context, key string) (string, error) {
	val, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", fmt.Errorf("key '%s' does not exist", key)
		}
		return "", fmt.Errorf("error getting value: %v", err)
	}
	return val, nil
}

// RPush pushes one or multiple values to the tail of a list
func (r *RedisClient) RPush(ctx context.Context, key string, values ...interface{}) error {
	err := r.client.RPush(ctx, key, values...).Err()
	if err != nil {
		return fmt.Errorf("error pushing to list: %v", err)
	}
	return nil
}

// LRange returns the specified elements of the list stored at the specified key
func (r *RedisClient) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	vals, err := r.client.LRange(ctx, key, start, stop).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("list '%s' does not exist", key)
		}
		return nil, fmt.Errorf("error getting list values: %v", err)
	}
	return vals, nil
}

// HMSet sets multiple fields of a hash
func (r *RedisClient) HMSet(ctx context.Context, key string, fields map[string]interface{}) error {
	err := r.client.HMSet(ctx, key, fields).Err()
	if err != nil {
		return fmt.Errorf("error setting hash fields: %v", err)
	}
	return nil
}

// HGet returns the value associated with field in the hash stored at key
func (r *RedisClient) HGet(ctx context.Context, key, field string) (string, error) {
	val, err := r.client.HGet(ctx, key, field).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", fmt.Errorf("field '%s' does not exist in hash '%s'", field, key)
		}
		return "", fmt.Errorf("error getting hash field value: %v", err)
	}
	return val, nil
}

// SAdd adds one or more members to a set
func (r *RedisClient) SAdd(ctx context.Context, key string, members ...interface{}) error {
	err := r.client.SAdd(ctx, key, members...).Err()
	if err != nil {
		return fmt.Errorf("error adding members to set: %v", err)
	}
	return nil
}

// SMembers returns all the members of the set stored at key
func (r *RedisClient) SMembers(ctx context.Context, key string) ([]string, error) {
	vals, err := r.client.SMembers(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("set '%s' does not exist", key)
		}
		return nil, fmt.Errorf("error getting set members: %v", err)
	}
	return vals, nil
}

// Publish publishes a message to the specified channel
func (r *RedisClient) Publish(ctx context.Context, channel string, message interface{}) error {
	err := r.client.Publish(ctx, channel, message).Err()
	if err != nil {
		return fmt.Errorf("error publishing message: %v", err)
	}
	return nil
}

// Subscribe subscribes to the specified channels
func (r *RedisClient) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return r.client.Subscribe(ctx, channels...)
}

// Close closes the Redis client connection
func (r *RedisClient) Close() error {
	err := r.client.Close()
	if err != nil {
		return fmt.Errorf("error closing Redis connection: %v", err)
	}
	return nil
}

// BeginTx starts a new transaction
func (r *RedisClient) BeginTx(ctx context.Context) redis.Pipeliner {
	return r.client.TxPipeline()
}

// ExecTx executes a transaction
func (r *RedisClient) ExecTx(ctx context.Context, tx *redis.Tx) ([]redis.Cmder, error) {
	// Commit the transaction
	cmders, err := tx.TxPipelined(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("error executing transaction: %v", err)
	}
	return cmders, nil
}

// Pipeline returns a new pipeline for issuing multiple Redis commands
func (r *RedisClient) Pipeline(ctx context.Context) redis.Pipeliner {
	return r.client.Pipeline()
}

// Incr increments the integer value of a key by one
func (r *RedisClient) Incr(ctx context.Context, key string) (int64, error) {
	val, err := r.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("error incrementing key '%s': %v", key, err)
	}
	return val, nil
}

// Decr decrements the integer value of a key by one
func (r *RedisClient) Decr(ctx context.Context, key string) (int64, error) {
	val, err := r.client.Decr(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("error decrementing key '%s': %v", key, err)
	}
	return val, nil
}

// Del deletes one or more keys
func (r *RedisClient) Del(ctx context.Context, keys ...string) error {
	err := r.client.Del(ctx, keys...).Err()
	if err != nil {
		return fmt.Errorf("error deleting keys: %v", err)
	}
	return nil
}
