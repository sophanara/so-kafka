package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"sokafka/share"
	"sync"
	"time"
)

type KafkaMessage = share.KafkaMessage

type Topic struct {
	Partitions map[int][]KafkaMessage
	Name       string
	mu         sync.RWMutex
}

type KafkaServer struct {
	Topics map[string]*Topic
	mu     sync.RWMutex
}

func NewKafkaServer() *KafkaServer {
	return &KafkaServer{
		Topics: make(map[string]*Topic),
	}
}

func (s *KafkaServer) CreateTopic(name string, numberPartition int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// check if topic exist
	if _, exists := s.Topics[name]; exists {
		return fmt.Errorf("topic %s alsready exist", name)
	}

	// create a new Topic
	topic := &Topic{
		Name:       name,
		Partitions: make(map[int][]KafkaMessage),
		mu:         sync.RWMutex{},
	}

	// initialize partitions
	for i := 0; i < numberPartition; i++ {
		topic.Partitions[i] = make([]KafkaMessage, 0)
	}
	s.Topics[name] = topic
	return nil
}

func (s *KafkaServer) StartServer() error {
	ln, err := net.Listen("tcp", ":9092")
	if err != nil {
		return err
	}
	defer ln.Close()

	fmt.Println("Server started...")
	fmt.Printf("waiting connection on port:%s\n", "9092")

	for {
		conn, err := ln.Accept()
		if err != nil {
			if err == io.EOF {
				return err
			}
			slog.Error("server accecpt error", "err", err)
		}

		// handle connection to this server.
		go s.handleConnection(conn)
	}
}

func (s *KafkaServer) handleConnection(conn net.Conn) {
	fmt.Println("new connection", conn.RemoteAddr())
	defer conn.Close()

	for {
		// read the message type
		msg, err := share.ReadProtocoleMessage(conn)
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading message: %v\n", err)
			}
			return
		}

		switch msg.MessageType {
		case share.ProduceRequest:
			err = s.handleProduceRequest(conn, msg.Payload)
			if err != nil {
				log.Printf("Error handleProduceRequest: %v\n", err)
				return
			}
		case share.ConsumeRequest:
			s.handleConsumerRequest(conn, msg.Payload)
		default:
			share.SendErrorResponse(conn, "Unknow message type.")
		}
	}
}

func (s *KafkaServer) handleProduceRequest(conn net.Conn, payload []byte) error {
	var req share.ProduceRequestMessage
	if err := json.Unmarshal(payload, &req); err != nil {
		return share.SendErrorResponse(conn, "Invalidate produce request format")
	}

	// for each meesage produce kakfka message
	var lastOffset int64
	var produceError error
	for _, msg := range req.Messages {
		err := s.Produce(req.Topic, req.Partition, msg.Value)
		if err != nil {
			produceError = err
			break
		}
		lastOffset++
	}

	// seend back to the client the response result of this produce request
	produceResponse := share.ProduceResponseMessage{
		Offset:  lastOffset,
		Success: produceError == nil,
		Error:   "",
	}

	if produceError != nil {
		produceResponse.Error = produceError.Error()
	}

	return share.SendResponse(conn, share.ProduceResponse, produceResponse)
}

func (s *KafkaServer) handleConsumerRequest(conn net.Conn, payload []byte) error {
	var req share.ConsumerRequestMessage
	if err := json.Unmarshal(payload, &req); err != nil {
		share.SendErrorResponse(conn, "Invalide consumer request  format")
	}

	messages, err := s.Consume(req.Topic, req.Partition, req.Offset)
	if err != nil {
		share.SendErrorResponse(conn, err.Error())
		return err
	}

	response := share.ConsumerResponseMessage{
		Error:    "",
		Messages: messages,
	}

	return share.SendResponse(conn, share.ConsumeResponse, response)
}

func (s *KafkaServer) Produce(topicName string, partition int, value []byte) error {
	s.mu.RLock()
	topic, exist := s.Topics[topicName]
	s.mu.RUnlock()

	if !exist {
		return fmt.Errorf("topic %s does not exist", topicName)
	}

	topic.mu.Lock()
	defer topic.mu.Unlock()

	messages, exist := topic.Partitions[partition]
	if !exist {
		return fmt.Errorf("partition %d does not exist", partition)
	}

	msg := KafkaMessage{
		Topic:     topicName,
		Partition: partition,
		Offset:    int64(len(messages)),
		Value:     value,
		Timestamp: time.Now(),
	}
	topic.Partitions[partition] = append(messages, msg)
	return nil
}

func (s *KafkaServer) Consume(topicName string, partition int, offset int64) ([]KafkaMessage, error) {
	s.mu.RLock()
	topic, exist := s.Topics[topicName]
	s.mu.RUnlock()

	if !exist {
		return nil, fmt.Errorf("topic %s does not exist", topicName)
	}

	topic.mu.RLock()
	defer topic.mu.RUnlock()

	messages, exist := topic.Partitions[partition]
	if !exist {
		return nil, fmt.Errorf("partition %d not exist", partition)
	}

	if offset >= int64(len(messages)) {
		return []KafkaMessage{}, nil
	}
	return messages[offset:], nil
}
