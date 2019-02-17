package nmq

// Consumer provides interface for consumer from queue
type Consumer interface {
	Do(interface{})
}