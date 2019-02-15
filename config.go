package nmq

// Config defines configuration for NMQ
type Config struct {
	RedisAddress  string
	RedisPassword string
	Name          string
	Channel       string
	RedisDB       int
}
