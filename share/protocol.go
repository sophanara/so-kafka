package share

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
)

// Mesage resperesent a Kafka Message
type KafkaMessage struct {
	// Topic     string
	Value     []byte
	Timestamp int64
	// Partition int
	Offset int64
	Size   uint32
}

// Protocole message type
const (
	ProduceRequest  = 1
	ProduceResponse = 2
	ConsumeRequest  = 3
	ConsumeResponse = 4
	ErrorResponse   = 5
)

type ProtocoleMessage struct {
	Payload     []byte
	MessageType uint8
}

type ProduceRequestMessage struct {
	Topic     string
	Messages  []KafkaMessage
	Partition int
}

type ProduceResponseMessage struct {
	Error   string
	Offset  int64
	Success bool
}

type ConsumerRequestMessage struct {
	Topic     string
	Partition int
	Offset    int64
	MaxBytes  int
}

type ConsumerResponseMessage struct {
	Error    string
	Messages []KafkaMessage
}

func SendResponse(conn net.Conn, messageType uint8, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return SendProtocoleMessage(conn, messageType, data)
}

func SendErrorResponse(conn net.Conn, errorMessage string) error {
	return SendResponse(conn, ErrorResponse, map[string]string{"erorr": errorMessage})
}

func ReadProtocoleMessage(r io.Reader) (*ProtocoleMessage, error) {
	// read the message type
	var msgType uint8
	if err := binary.Read(r, binary.BigEndian, &msgType); err != nil {
		return nil, err
	}

	// read the payload length
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	// read the payload
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	result := &ProtocoleMessage{
		MessageType: msgType,
		Payload:     payload,
	}

	return result, nil
}

func SendProtocoleMessage(conn net.Conn, messageType uint8, payload []byte) error {
	// write the message type
	if err := binary.Write(conn, binary.BigEndian, messageType); err != nil {
		return err
	}

	// write the payload length
	if err := binary.Write(conn, binary.BigEndian, uint32(len(payload))); err != nil {
		return err
	}

	// write payload
	_, err := conn.Write(payload)
	return err
}
