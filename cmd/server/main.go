package main

import (
	"log"
	"sokafka/server"
)

func main() {
	server := server.NewKafkaServer()

	if err := server.CreateTopic("test-partition", 3); err != nil {
		log.Fatal(err)
	}

	log.Fatal(server.StartServer())
}
