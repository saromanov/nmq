package main

import (
	"github.com/saromanov/nmq"
)

func main() {
	client, err := nmq.New(&nmq.Config{
		RedisAddress: "localhost:6379",
	})
	if err != nil {
		panic(err)
	}
	client.Publish("data", "value")
}
