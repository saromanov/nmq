package main

import (
	"fmt"

	"github.com/saromanov/nmq"
)

type Consumer struct {
}

func (c Consumer) Do(m *nmq.Message) {
	fmt.Println(m)
}

func main() {
	client, err := nmq.New(&nmq.Config{
		RedisAddress: "localhost:6379",
	})
	if err != nil {
		panic(err)
	}
	var c Consumer
	client.AddChannel("data")
	err = client.AddConsumer("first", c)
	if err != nil {
		panic(err)
	}
	client.Start()
}
