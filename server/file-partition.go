package server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

// IndexEntry represents an entry in the index file
type IndexEntry struct {
	Offset    int64
	Position  int64
	Timestamp int64
}
type FilePartition struct {
	dataFile    *os.File
	indexFile   *os.File
	offsetFile  *os.File
	baseDir     string
	topic       string
	partitionID int
	mutex       sync.Mutex
}

func (f *FilePartition) Close() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if err := f.dataFile.Close(); err != nil {
		return err
	}
	if err := f.indexFile.Close(); err != nil {
		return err
	}

	return f.offsetFile.Close()
}

// Helper functions to retrieved the current offset from the offsetfile
func (p *FilePartition) getCurrentOffset() (int64, error) {
	// put back the pointer top the begining of the file
	if _, err := p.offsetFile.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	var offset int64
	err := binary.Read(p.offsetFile, binary.BigEndian, &offset)
	if err != nil {
		// If file is empty, start from 0
		if err.Error() == "EOF" {
			return 0, nil
		}
		return 0, err
	}

	return offset, nil
}

// Append adds a new message to the partition
func (p *FilePartition) Append(value []byte, timestamp int64) (int64, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Get current offset
	offset, err := p.getCurrentOffset()
	if err != nil {
		return 0, err
	}

	// Get current position in data file
	position, err := p.dataFile.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	// Create message record
	record := KafkaMessage{
		Size:      uint32(len(value)),
		Timestamp: timestamp,
		Offset:    offset,
		Value:     value,
	}

	// Write message record
	if err := p.writeMessageRecord(record); err != nil {
		return 0, err
	}

	// Create and write index entry
	entry := IndexEntry{
		Offset:    offset,
		Position:  position,
		Timestamp: timestamp,
	}

	if err := p.writeIndexEntry(entry); err != nil {
		return 0, err
	}

	// Update offset
	offset = offset + 1
	if err := p.updateOffset(offset); err != nil {
		return 0, err
	}

	fmt.Printf("Offset retrieved from file : %d \n", offset)
	return offset, nil
}

func (p *FilePartition) writeIndexEntry(entry IndexEntry) error {
	return json.NewEncoder(p.indexFile).Encode(entry)
}

func (p *FilePartition) updateOffset(offset int64) error {
	if _, err := p.offsetFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return binary.Write(p.offsetFile, binary.BigEndian, offset)
}

func (p *FilePartition) writeMessageRecord(record KafkaMessage) error {
	// Write size
	if err := binary.Write(p.dataFile, binary.BigEndian, record.Size); err != nil {
		return err
	}

	// Write timestamp
	if err := binary.Write(p.dataFile, binary.BigEndian, record.Timestamp); err != nil {
		return err
	}

	// Write offset
	if err := binary.Write(p.dataFile, binary.BigEndian, record.Offset); err != nil {
		return err
	}

	// Write value
	_, err := p.dataFile.Write(record.Value)
	return err
}

// Read reads messages starting from a given offset
func (p *FilePartition) Read(startOffset int64, maxBytes int) ([]KafkaMessage, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Find position for startOffset using index
	position, err := p.findPosition(startOffset)
	if err != nil {
		return nil, err
	}

	// Seek to position
	if _, err := p.dataFile.Seek(position, io.SeekStart); err != nil {
		return nil, err
	}

	var messages []KafkaMessage
	bytesRead := 0

	for bytesRead < maxBytes {
		record, bytes, err := p.readMessageRecord()
		if err != nil {
			break // EOF or other error
		}

		bytesRead += bytes
		if bytesRead > maxBytes {
			break
		}

		messages = append(messages, record)
	}

	return messages, nil
}

func (p *FilePartition) findPosition(offset int64) (int64, error) {
	// Reset index file position
	if _, err := p.indexFile.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	decoder := json.NewDecoder(p.indexFile)
	for {
		var entry IndexEntry
		if err := decoder.Decode(&entry); err != nil {
			return 0, err
		}
		if entry.Offset >= offset {
			return entry.Position, nil
		}
	}
}

func (p *FilePartition) readMessageRecord() (KafkaMessage, int, error) {
	var record KafkaMessage
	var bytesRead int

	// Read size
	if err := binary.Read(p.dataFile, binary.BigEndian, &record.Size); err != nil {
		return record, bytesRead, err
	}
	bytesRead += 4

	// Read timestamp
	if err := binary.Read(p.dataFile, binary.BigEndian, &record.Timestamp); err != nil {
		return record, bytesRead, err
	}
	bytesRead += 8

	// Read offset
	if err := binary.Read(p.dataFile, binary.BigEndian, &record.Offset); err != nil {
		return record, bytesRead, err
	}
	bytesRead += 8

	// Read value
	record.Value = make([]byte, record.Size)
	n, err := p.dataFile.Read(record.Value)
	if err != nil {
		return record, bytesRead, err
	}
	bytesRead += n

	return record, bytesRead, nil
}
