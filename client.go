package main

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

type KafkaClient struct {
	conn net.Conn
}

func NewKafkaClient(address string) (*KafkaClient, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	return &KafkaClient{conn: conn}, nil
}

func sendRequest(conn net.Conn, messageType uint8, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return sendProtocoleMessage(conn, messageType, data)
}

func (c *KafkaClient) Produce(topic string, partition int, messages [][]byte) (int64, error) {
	kafkaMessages := make([]KafkaMessage, len(messages))
	for i, msg := range messages {
		kafkaMessages[i] = KafkaMessage{
			Topic:     topic,
			Partition: partition,
			Value:     msg,
			Timestamp: time.Now(),
		}
	}

	req := ProduceRequestMessage{
		Topic:     topic,
		Partition: partition,
		Messages:  kafkaMessages,
	}

	if err := sendRequest(c.conn, ProduceRequest, req); err != nil {
		return 0, err
	}

	msg, err := readProtocoleMessage(c.conn)
	if err != nil {
		return 0, err
	}

	var resp ProduceResponseMessage
	if err := json.Unmarshal(msg.Payload, &resp); err != nil {
		return 0, err
	}

	if !resp.Success {
		return 0, fmt.Errorf("produce failed: %s", resp.Error)
	}
	return resp.Offset, nil
}

func (c *KafkaClient) Consume(topic string, partition int, offset int64, maxBytes int) ([]KafkaMessage, error) {
	req := ConsumerRequestMessage{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		MaxBytes:  maxBytes,
	}

	if err := sendRequest(c.conn, ConsumeRequest, req); err != nil {
		return nil, err
	}

	msg, err := readProtocoleMessage(c.conn)
	if err != nil {
		return nil, err
	}

	var resp ConsumerResponseMessage
	if err := json.Unmarshal(msg.Payload, &resp); err != nil {
		return nil, err
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("consume response failed: %s", resp.Error)
	}
	return resp.Messages, nil
}

func (c *KafkaClient) Close() error {
	return c.conn.Close()
}
