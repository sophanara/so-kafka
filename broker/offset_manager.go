package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type OffsetManager struct {
	mu          sync.RWMutex
	offsets     map[string]map[string]map[int]int64 // clientID -> topic -> partition -> offset
	offsetsFile string                              // path to offsets file
}

func NewOffsetManager(baseDir string) *OffsetManager {
	offsetsFile := filepath.Join(baseDir, "offsets.json")
	om := &OffsetManager{
		offsets:     make(map[string]map[string]map[int]int64),
		offsetsFile: offsetsFile,
	}

	// Load saved offsets if they exist
	if err := om.loadOffsets(); err != nil {
		fmt.Printf("Warning: Could not load saved offsets: %v\n", err)
	}

	return om
}

func (om *OffsetManager) loadOffsets() error {
	data, err := os.ReadFile(om.offsetsFile)
	if os.IsNotExist(err) {
		return nil // File doesn't exist yet, that's OK
	}
	if err != nil {
		return fmt.Errorf("failed to read offsets file: %w", err)
	}

	return json.Unmarshal(data, &om.offsets)
}

func (om *OffsetManager) saveOffsets() error {
	data, err := json.Marshal(om.offsets)
	if err != nil {
		return fmt.Errorf("failed to marshal offsets: %w", err)
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(om.offsetsFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write to temporary file first
	tempFile := om.offsetsFile + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}

	// Rename temporary file to actual file (atomic operation)
	if err := os.Rename(tempFile, om.offsetsFile); err != nil {
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	return nil
}

func (om *OffsetManager) GetOffset(clientID, topic string, partition int) int64 {
	om.mu.RLock()
	defer om.mu.RUnlock()

	if _, exists := om.offsets[clientID]; !exists {
		return 0
	}
	if _, exists := om.offsets[clientID][topic]; !exists {
		return 0
	}
	return om.offsets[clientID][topic][partition]
}

func (om *OffsetManager) CommitOffset(clientID, topic string, partition int, offset int64) {
	om.mu.Lock()
	defer om.mu.Unlock()

	if _, exists := om.offsets[clientID]; !exists {
		om.offsets[clientID] = make(map[string]map[int]int64)
	}
	if _, exists := om.offsets[clientID][topic]; !exists {
		om.offsets[clientID][topic] = make(map[int]int64)
	}
	om.offsets[clientID][topic][partition] = offset

	// Save offsets to disk after each commit
	if err := om.saveOffsets(); err != nil {
		fmt.Printf("Warning: Failed to save offsets: %v\n", err)
	}
}
