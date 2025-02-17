package broker

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
	"strconv"
	"strings"
	"sync"
	"time"
)

type KafkaMessage = share.KafkaMessage

type Topic struct {
	FilePartitions map[int]*FilePartition
	Name           string
	mu             sync.RWMutex
}

type KafkaBroker struct {
	Topics    map[string]*Topic
	mu        sync.RWMutex
	offsetMgr *OffsetManager
	basePath  string
}

func NewKafkaBroker() *KafkaBroker {
	currentPath, err := os.Getwd()

	if err != nil {
		currentPath = "tmp/kafka"
	} else {
		currentPath = filepath.Join(currentPath, "tmp/kafka")
	}
	return &KafkaBroker{
		Topics:    make(map[string]*Topic),
		offsetMgr: NewOffsetManager(currentPath),
		basePath:  currentPath,
	}
}

func (s *KafkaBroker) createFilePartition(baseDir string, topic string, partitionId int) (*FilePartition, error) {
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

func (s *KafkaBroker) Close(topicName string) {
	fileParts := s.Topics[topicName].FilePartitions
	for k := range fileParts {
		if err := fileParts[k].Close(); err != nil {
			fmt.Printf("Failed to close the partitions[%d]: %v", k, err)
		}
	}
}

func (s *KafkaBroker) CreateTopic(topicName string, numberPartition int) error {
	fmt.Println("Starting the creation of the file topic structure...")
	s.mu.Lock()
	defer s.mu.Unlock()

	currentDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current directory: %v", err)
	}

	fmt.Printf("Current working directory: %s\n", currentDir)

	//Load existing topics and partitions
	if err := s.loadExistingTopics(); err != nil {
		log.Printf("Error laoding existing tiopics: %v", err)
	}

	// check if the topic has been created
	if _, exist := s.Topics[topicName]; exist {
		log.Printf("topic %s already exist", topicName)
		return nil
	}

	topic := &Topic{
		FilePartitions: make(map[int]*FilePartition),
		Name:           topicName,
		mu:             sync.RWMutex{},
	}

	topic.mu.Lock()
	defer topic.mu.Unlock()

	// create all the partitions
	for i := 0; i < numberPartition; i++ {
		filePartition, err := s.createFilePartition(s.basePath, topicName, i)
		if err != nil {
			return err
		}
		topic.FilePartitions[i] = filePartition
	}

	s.Topics[topicName] = topic
	return nil
}

func (s *KafkaBroker) StartServer() error {

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

func (s *KafkaBroker) handleConnection(conn net.Conn) {
	fmt.Println("new connection", conn.RemoteAddr())
	defer conn.Close()

	for {
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
		case share.ConsumeRequest:
			err = s.handleConsumerRequest(conn, msg.Payload)
		case share.CommitOffset:
			err = s.handleCommitOffset(conn, msg.Payload)
		default:
			err = share.SendErrorResponse(conn, "Unknown message type.")
		}

		if err != nil {
			log.Printf("Error handling request: %v\n", err)
			return
		}
	}
}

func (s *KafkaBroker) handleProduceRequest(conn net.Conn, payload []byte) error {
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

	// send back to the client the response result of this produce request
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

func (s *KafkaBroker) handleConsumerRequest(conn net.Conn, payload []byte) error {
	var req share.ConsumerRequestMessage
	if err := json.Unmarshal(payload, &req); err != nil {
		if err = share.SendErrorResponse(conn, "Invalid consumer request format"); err != nil {
			return err
		}
		return err
	}

	// Get offset from offset manager
	offset := s.offsetMgr.GetOffset(req.ClientID, req.Topic, req.Partition)
	fmt.Printf("Consume offset: %d \n", offset)

	messages, err := s.Consume(req.Topic, req.Partition, offset, req.MaxBytes)
	if err != nil {
		if err = share.SendErrorResponse(conn, err.Error()); err != nil {
			return err
		}
		return err
	}

	// Update offset if messages were returned
	if len(messages) > 0 {
		lastMsg := messages[len(messages)-1]
		s.offsetMgr.CommitOffset(req.ClientID, req.Topic, req.Partition, lastMsg.Offset+1)
	}

	response := share.ConsumerResponseMessage{
		Error:    "",
		Messages: messages,
	}

	return share.SendResponse(conn, share.ConsumeResponse, response)
}

func (s *KafkaBroker) handleCommitOffset(conn net.Conn, payload []byte) error {
	var req share.CommitOffsetMessage
	if err := json.Unmarshal(payload, &req); err != nil {
		return share.SendErrorResponse(conn, "Invalid commit offset request format")
	}

	s.offsetMgr.CommitOffset(req.ClientID, req.Topic, req.Partition, req.Offset)

	response := share.CommitOffsetResponse{
		Success: true,
	}

	return share.SendResponse(conn, share.CommitOffset, response)
}

func (s *KafkaBroker) produce(topicName string, partition int, value []byte) (int64, error) {
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

func (s *KafkaBroker) Consume(topicName string, partition int, offset int64, maxBytes int) ([]KafkaMessage, error) {
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

func (s *KafkaBroker) loadExistingTopics() error {
	log.Println("Loading existing topics")
	dir := s.basePath
	// read from the base directory
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			//create the base directory if if doesn't exist
			return os.MkdirAll(dir, 0755)
		}
		return err
	}

	// Iterator of the directory entries to find the topics
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		topicName := entry.Name()
		// Read topic directory to count the number of partitions

		partitionEntries, err := os.ReadDir(filepath.Join(dir, topicName))
		if err != nil {
			continue // skip if can't read topic directory
		}

		// Create topic with existing partitions
		topic := &Topic{
			Name:           topicName,
			FilePartitions: make(map[int]*FilePartition),
			mu:             sync.RWMutex{},
		}

		// Load each partition
		for _, partEntry := range partitionEntries {
			if strings.HasPrefix(partEntry.Name(), "partition-") {
				partIdString := strings.TrimPrefix(partEntry.Name(), "partition-")
				partId, err := strconv.Atoi(partIdString)
				if err != nil {
					log.Printf("Warning: Invalid partition name %s: %v", partEntry.Name(), err)
					continue // Skip invalid partition names
				}

				filePartition, err := s.createFilePartition(dir, topicName, partId)
				if err != nil {
					continue //Skip failed partitions load
				}
				topic.FilePartitions[partId] = filePartition
			}
		}

		//Add the topic to broker if it has any valid partitions
		if len(topic.FilePartitions) > 0 {
			s.Topics[topicName] = topic
		}
	}
	return nil
}
