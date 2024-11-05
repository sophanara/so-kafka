package main

import (
	"fmt"
	"log"
	"time"
)

func main() {
	server := NewKafkaServer()

	if err := server.CreateTopic("test-partition", 3); err != nil {
		log.Fatal(err)
	}

	go server.StartServer()
	time.Sleep(time.Second) // waut for the server to start

	// create a KafkaClient to test our Custom server
	client, err := NewKafkaClient("localhost:9092")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	///produce messages
	messages := [][]byte{
		[]byte("Hello"),
		[]byte("World"),
	}

	offset, err := client.Produce("test-partition", 0, messages)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Produce messages, offset: %d\n", offset)

	// consume messages
	consumed, err := client.Consume("test-partition", 0, 0, 1024)
	if err != nil {
		log.Fatal(err)
	}
	for _, msg := range consumed {
		fmt.Printf("Consumed messages: %s\n", string(msg.Value))
	}
}
