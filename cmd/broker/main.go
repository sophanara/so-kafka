package main

import (
	"log"
	"sokafka/broker"
)

func main() {
	server := broker.NewKafkaBroker()

	err := server.CreateTopic("test-partition", 3)
	if err != nil {
		log.Fatal(err)
	}

	defer server.Close("test-partition")

	log.Fatal(server.StartServer())
}
