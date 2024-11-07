package main

import (
	"fmt"
	"log"
	"sokafka/client"
)

func main() {
	// create a KafkaClient to test our Custom server
	client, err := client.NewKafkaClient("localhost:9092")
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
	offset, err = client.Produce("test-partition", 0, messages)
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
