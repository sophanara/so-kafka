package client

import (
	"encoding/json"
	"fmt"
	"net"
	"sokafka/share"
	"time"
)

type KafkaClient struct {
	conn     net.Conn
	clientID string
}

func NewKafkaClient(address string, clientID string) (*KafkaClient, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	return &KafkaClient{
		conn:     conn,
		clientID: clientID,
	}, nil
}

func sendRequest(conn net.Conn, messageType uint8, payload interface{}) error {
	fmt.Printf("sendRequest: %d %v\n", messageType, payload)
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return share.SendProtocoleMessage(conn, messageType, data)
}

func (c *KafkaClient) Produce(topic string, partition int, messages [][]byte) (int64, error) {
	kafkaMessages := make([]share.KafkaMessage, len(messages))
	for i, msg := range messages {
		kafkaMessages[i] = share.KafkaMessage{
			Value:     msg,
			Timestamp: time.Now().Unix(),
		}
	}

	req := share.ProduceRequestMessage{
		Topic:     topic,
		Partition: partition,
		Messages:  kafkaMessages,
	}

	if err := sendRequest(c.conn, share.ProduceRequest, req); err != nil {
		return 0, err
	}

	msg, err := share.ReadProtocoleMessage(c.conn)
	if err != nil {
		return 0, err
	}

	var resp share.ProduceResponseMessage
	if err := json.Unmarshal(msg.Payload, &resp); err != nil {
		return 0, err
	}

	if !resp.Success {
		return 0, fmt.Errorf("produce failed: %s", resp.Error)
	}
	return resp.Offset, nil
}

func (c *KafkaClient) Consume(topic string, partition int, maxBytes int) ([]share.KafkaMessage, error) {
	req := share.ConsumerRequestMessage{
		Topic:     topic,
		Partition: partition,
		ClientID:  c.clientID,
		MaxBytes:  maxBytes,
	}

	if err := sendRequest(c.conn, share.ConsumeRequest, req); err != nil {
		return nil, err
	}

	msg, err := share.ReadProtocoleMessage(c.conn)
	if err != nil {
		return nil, err
	}

	var resp share.ConsumerResponseMessage
	if err := json.Unmarshal(msg.Payload, &resp); err != nil {
		return nil, err
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("consume response failed: %s", resp.Error)
	}
	return resp.Messages, nil
}

func (c *KafkaClient) CommitOffset(topic string, partition int, offset int64) error {
	req := share.CommitOffsetMessage{
		Topic:     topic,
		Partition: partition,
		ClientID:  c.clientID,
		Offset:    offset,
	}

	if err := sendRequest(c.conn, share.CommitOffset, req); err != nil {
		return err
	}

	msg, err := share.ReadProtocoleMessage(c.conn)
	if err != nil {
		return err
	}

	var resp share.CommitOffsetResponse
	if err := json.Unmarshal(msg.Payload, &resp); err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("commit offset failed: %s", resp.Error)
	}
	return nil
}

// Add a method to consume from beginning
func (c *KafkaClient) ConsumeFromOffset(topic string, partition int, maxBytes int, offset int64) ([]share.KafkaMessage, error) {
	// First commit the special offset to tell the broker to reset
	err := c.CommitOffset(topic, partition, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to reset offset: %w", err)
	}

	// Then consume normally - the broker will use offset 0
	return c.Consume(topic, partition, maxBytes)
}

func (c *KafkaClient) Close() error {
	return c.conn.Close()
}
