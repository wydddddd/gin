package cache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// FilesystemStore implements the cache Store interface using the local
// filesystem. Each cache entry is stored as a JSON file.
type FilesystemStore struct {
	basePath   string
	defaultTTL time.Duration
}

// NewFilesystemStore creates a filesystem-backed cache store.
// It creates the base directory if it doesn't exist.
func NewFilesystemStore(basePath string, ttl time.Duration) (*FilesystemStore, error) {
	if err := os.MkdirAll(basePath, 0777); err != nil {
		return nil, fmt.Errorf("cache: failed to create directory %s: %w", basePath, err)
	}
	return &FilesystemStore{
		basePath:   basePath,
		defaultTTL: ttl,
	}, nil
}

func (s *FilesystemStore) filePath(key string) string {
	return filepath.Join(s.basePath, key+".json")
}

// Get retrieves a cached response from the filesystem.
func (s *FilesystemStore) Get(key string) (*CachedResponse, error) {
	path := s.filePath(key)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("cache: failed to read %s: %w", path, err)
	}

	var resp CachedResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("cache: failed to unmarshal %s: %w", path, err)
	}

	// Remove expired entries on access
	if resp.IsExpired() {
		os.Remove(path)
		return nil, nil
	}

	return &resp, nil
}

// Set stores a response on the filesystem.
func (s *FilesystemStore) Set(key string, resp *CachedResponse, ttl time.Duration) error {
	path := s.filePath(key)

	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("cache: failed to marshal response: %w", err)
	}

	return os.WriteFile(path, data, 0666)
}

// Delete removes a cached entry from the filesystem.
func (s *FilesystemStore) Delete(key string) error {
	path := s.filePath(key)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("cache: failed to delete %s: %w", path, err)
	}
	return nil
}

// Clear removes all cached entries by deleting the entire cache directory.
func (s *FilesystemStore) Clear() error {
	return os.RemoveAll(s.basePath)
}

// PurgeExpired scans the cache directory and removes any expired entries.
// Returns the number of entries purged.
func (s *FilesystemStore) PurgeExpired() (int, error) {
	entries, err := os.ReadDir(s.basePath)
	if err != nil {
		return 0, fmt.Errorf("cache: failed to read directory: %w", err)
	}

	purged := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := filepath.Join(s.basePath, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		var resp CachedResponse
		if err := json.Unmarshal(data, &resp); err != nil {
			continue
		}

		if resp.IsExpired() {
			os.Remove(path)
			purged++
		}
	}
	return purged, nil
}

// DiskUsage returns the total size in bytes of all cache files.
func (s *FilesystemStore) DiskUsage() (int64, error) {
	var total int64
	entries, err := os.ReadDir(s.basePath)
	if err != nil {
		return 0, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total, nil
}
