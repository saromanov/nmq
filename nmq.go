// Package nmq provides definition of the NMQ(new message queue) based on Redis
package nmq

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

const redisAddress = "127.0.0.1:6379"

var (
	errNoChannel = errors.New("channel if not found")
)
// Queue defines abstraction for nmq
type Queue interface {
	AddConsumer(string, Consumer) error
	AddChannel(string) error
	Publish(string, interface{}) error
	Start() error
}

// nmq is a main struct for app
type nmq struct {
	client      *redis.Client
	name        string
	channels    []string
	mu          *sync.Mutex
	doneMessage chan *Message
	consumers   map[string]chan *Message
	poolTime    time.Duration
}

// New provides initialization of the app
func New(c *Config) (Queue, error) {
	if c == nil {
		return nil, errors.New("config is not defined")
	}
	if c.RedisAddress == "" {
		c.RedisAddress = redisAddress
	}
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
		doneMessage: make(chan *Message),
		mu:          &sync.Mutex{},
		poolTime:    500 * time.Millisecond,
		consumers:   make(map[string]chan *Message),
	}, nil
}

// Start provides starting of consuming
func (n *nmq) Start() error {
	n.consume()
	return nil
}

// Publish provides publishing of data
func (n *nmq) Publish(name string, payload interface{}) error {
	if _, err := n.client.LPush(name, payload).Result(); err != nil {
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
	if _, err := n.client.SAdd(name, name).Result(); err != nil {
		return fmt.Errorf("unable to add consumer %s: %v", name, err)
	}
	doneMessage := make(chan *Message)
	n.consumers[name] = doneMessage
	go n.processConsume(consumer, doneMessage)
	return nil
}

// RemoveConsumer provides removing of consumer from the list
func (n *nmq) RemoveConsumer(name string) error {
	if _, err := n.client.SRem(name, name).Result(); err != nil {
		return fmt.Errorf("unable to remove consumer %s: %v", name, err)
	}
	return nil
}

// AddChannel provides adding of the channel for consuming
func (n *nmq) AddChannel(name string) error {
	n.channels = append(n.channels, name)
	return nil
}

// RemoveChannel provides removing of the channel from list
func (n *nmq) RemoveChannel(name string) error {
	for i, channel := range n.channels {
		if channel == name {
			n.channels = append(n.channels[:i], n.channels[i+1:]...)
			return nil
		}
	}
	return errNoChannel
}

// processConsume provides waiting of consumer channel and
// sending message to consumer func
func (n *nmq) processConsume(consumer Consumer, doneMessage chan *Message) {
	for data := range doneMessage {
		consumer.Do(data)
	}
}

// consume provides consuming of the messages
// currently at draft
// Not checking of consumeKey error, because in the case of no
// new messages, its return nil
func (n *nmq) consume() {
	for {

		for i := 0; i < len(n.channels); i++ {
			n.consumeKey(n.channels[i]) // nolint
		}

		//time.Sleep(n.poolTime)
	}
}

// consumeKey provides consuming of the key
func (n *nmq) consumeKey(key string) error {
	value, err := n.client.RPopLPush(key, "").Result()
	if err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, doneMessage := range n.consumers {
		doneMessage <- &Message{
			data: value,
			key:  key,
		}
	}

	return nil
}
