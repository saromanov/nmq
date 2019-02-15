// Package nmq provides definition of the NMQ(new message queue) based on Redis
package nmq

import (
	"sync"
	"fmt"
	"github.com/go-redis/redis"
)

// nmq is a main struct for app
type nmq struct {
	name string
	channel string
	mu sync.Mutex
	doneMessage chan *Message
}
