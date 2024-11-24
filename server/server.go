package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sokafka/share"
	"sync"
	"time"
)

type KafkaMessage = share.KafkaMessage

type Topic struct {
	FilePartitions map[int]*FilePartition
	Name           string
	mu             sync.RWMutex
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

func (s *KafkaServer) createFilePartition(baseDir string, topic string, partitionId int) (*FilePartition, error) {
	fp := &FilePartition{
		baseDir:     baseDir,
		topic:       topic,
		partitionID: partitionId,
	}

	// create the directory structure to create the topic and partition
	dir := filepath.Join(baseDir, topic, fmt.Sprintf("partition-%v", partitionId))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directoryt: %v", err)
	}

	// Open data file
	dataPath := filepath.Join(dir, "data.log")
	dataFile, err := os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %v", err)
	}

	// open index.idx
	dataPath = filepath.Join(dir, "index.idx")
	indexFile, err := os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("failed to open  indexFile: %v", err)
	}

	// create offset.meta
	dataPath = filepath.Join(dir, "offset.meta")
	offsetFile, err := os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		dataFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("failed to open offset.meta: %v", err)
	}

	fp.dataFile = dataFile
	fp.indexFile = indexFile
	fp.offsetFile = offsetFile

	return fp, nil
}

func (s *KafkaServer) Close(topicName string) {
	fileParts := s.Topics[topicName].FilePartitions
	for k := range fileParts {
		if err := fileParts[k].Close(); err != nil {
			fmt.Printf("Failed to close the partitions[%d]: %v", k, err)
		}
	}
}

func (s *KafkaServer) CreateTopic(topicName string, numberPartition int) error {
	fmt.Println("Starting the creation of the file topic structure...")
	s.mu.Lock()
	defer s.mu.Unlock()

	// check if the topic has been created
	if _, exist := s.Topics[topicName]; exist {
		return fmt.Errorf("topic %s already exist", topicName)
	}

	topic := &Topic{
		FilePartitions: make(map[int]*FilePartition),
		Name:           topicName,
		mu:             sync.RWMutex{},
	}

	baseDir := "tmp/kafka"

	topic.mu.Lock()
	defer topic.mu.Unlock()

	// create all the partitions
	for i := 0; i < numberPartition; i++ {
		filePartition, err := s.createFilePartition(baseDir, topicName, i)
		if err != nil {
			return err
		}
		topic.FilePartitions[i] = filePartition
	}

	s.Topics[topicName] = topic
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
			err = s.handleConsumerRequest(conn, msg.Payload)
			if err != nil {
				log.Printf("Error handling consumer request: %v\n", err)
				return
			}
		default:
			err = share.SendErrorResponse(conn, "Unknow message type.")
			if err != nil {
				log.Printf("Error handling response: %v\n", err)
				return
			}
		}
	}
}

func (s *KafkaServer) handleProduceRequest(conn net.Conn, payload []byte) error {
	var req share.ProduceRequestMessage
	if err := json.Unmarshal(payload, &req); err != nil {
		return share.SendErrorResponse(conn, "Invalidate produce request format")
	}

	// for each message produce kakfka message
	var lastOffset int64
	var produceError error
	for _, msg := range req.Messages {
		offset, err := s.produce(req.Topic, req.Partition, msg.Value)
		if err != nil {
			produceError = err
			break
		}
		lastOffset = offset
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
		if err = share.SendErrorResponse(conn, "Invalid consumer request  format"); err != nil {
			return err
		}

		return err
	}

	messages, err := s.Consume(req.Topic, req.Partition, req.Offset, req.MaxBytes)
	if err != nil {
		if err = share.SendErrorResponse(conn, err.Error()); err != nil {
			return err
		}
		return err
	}

	response := share.ConsumerResponseMessage{
		Error:    "",
		Messages: messages,
	}

	return share.SendResponse(conn, share.ConsumeResponse, response)
}

func (s *KafkaServer) produce(topicName string, partition int, value []byte) (int64, error) {
	s.mu.RLock()
	topic, exist := s.Topics[topicName]
	s.mu.RUnlock()

	if !exist {
		return 0, fmt.Errorf("topic %s does not exist", topicName)
	}

	topic.mu.RLock()
	filePartition, exist := topic.FilePartitions[partition]
	topic.mu.RUnlock()

	if !exist {
		return 0, fmt.Errorf("partition %d does not exist", partition)
	}

	offset, err := filePartition.Append(value, time.Now().Unix())
	if err != nil {
		return 0, err
	}
	fmt.Printf("Written message at offset:%d\n", offset)
	return offset, nil
}

func (s *KafkaServer) Consume(topicName string, partition int, offset int64, maxBytes int) ([]KafkaMessage, error) {
	s.mu.RLock()
	topic, exist := s.Topics[topicName]
	s.mu.RUnlock()

	if !exist {
		return nil, fmt.Errorf("topic %s does not exist", topicName)
	}

	topic.mu.RLock()
	filePartition, exist := topic.FilePartitions[partition]
	topic.mu.RUnlock()

	if !exist {
		return nil, fmt.Errorf("partition %d not exist", partition)
	}

	messages, err := filePartition.Read(offset, maxBytes)
	if err != nil {
		return nil, err
	}

	return messages, nil
}
