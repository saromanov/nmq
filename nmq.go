// Package nmq provides definition of the NMQ(new message queue) based on Redis
package nmq

import (
	"fmt"
	"sync"

	"github.com/go-redis/redis"
)

// Queue defines abstraction for nmq
type Queue interface {
	Publish(string, interface{}) error
}

// nmq is a main struct for app
type nmq struct {
	client      *redis.Client
	name        string
	channel     string
	mu          *sync.Mutex
	doneMessage chan *Message
}

// New provides initialization of the app
func New(c *Config) (Queue, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     c.RedisAddress,
		Password: c.RedisPassword,
		DB:       c.RedisDB,
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, fmt.Errorf("unable to init redis client: %v", err)
	}
	return &nmq{
		client:      client,
		name:        c.Name,
		channel:     c.Channel,
		doneMessage: make(chan *Message),
		mu:          &sync.Mutex{},
	}, nil
}

// Publish provides publishing of data
func (n *nmq) Publish(name string, payload interface{}) error {
	_, err := n.client.LPush(name, payload).Result()
	if err != nil {
		return fmt.Errorf("unable to push data: %v", err)
	}
	return nil
}

// String returns name of the queue
func (n *nmq) String() string {
	return fmt.Sprintf("queue: %s", n.name)
}

// AddConsumer provides adding a new consumers
func (n *nmq) AddConsumer(name string, consumer Consumer) error {
	if _, err := n.client.SAdd(queue.consumersKey, name).Result(); err != nil {
		return fmt.Errorf("unable to add consumer %s: %v", name, e)
	}
	return nil
}

// consume provides consuming of the messages
// currently at draft
func (n *nmq) consume(key, data string) {
	for {
		value, err := n.client.RPopLPush(key, data).Result()
		if err != nil {
			return
		}
		fmt.Println(value)
	}
}
