package main

import (
	"log"
	"sokafka/server"
)

func main() {
	server := server.NewKafkaServer()

	err := server.CreateTopic("test-partition", 3)
	if err != nil {
		log.Fatal(err)
	}

	defer server.Close("test-partition")

	log.Fatal(server.StartServer())
}
