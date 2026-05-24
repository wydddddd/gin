package cache

import (
	"container/list"
	"sync"
	"time"
)

type entry struct {
	key     string
	value   *CachedResponse
	element *list.Element
}

// MemoryStore implements an in-memory LRU cache backend.
type MemoryStore struct {
	capacity int
	items    map[string]*entry
	order    *list.List
	mu       sync.Mutex
	stopCh   chan struct{}
}

// NewMemoryStore creates a new in-memory cache with the given capacity.
// It starts a background goroutine to periodically evict expired entries.
func NewMemoryStore(capacity int) *MemoryStore {
	s := &MemoryStore{
		capacity: capacity,
		items:    make(map[string]*entry),
		order:    list.New(),
		stopCh:   make(chan struct{}),
	}
	go s.cleanup()
	return s
}

func (s *MemoryStore) cleanup() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			now := time.Now()
			for key, e := range s.items {
				if now.Sub(e.value.Created) > e.value.TTL {
					s.order.Remove(e.element)
					delete(s.items, key)
				}
			}
			s.mu.Unlock()
		case <-s.stopCh:
			return
		}
	}
}

// Get retrieves a cached response by key. Returns nil if not found.
func (s *MemoryStore) Get(key string) (*CachedResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.items[key]
	if !ok {
		return nil, nil
	}

	// Promote to front of LRU list
	s.order.MoveToFront(e.element)
	return e.value, nil
}

// Set stores a response in the cache. If the cache is at capacity,
// the least recently used entry is evicted.
func (s *MemoryStore) Set(key string, resp *CachedResponse, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update existing entry
	if e, ok := s.items[key]; ok {
		e.value = resp
		s.order.MoveToFront(e.element)
		return nil
	}

	// Evict if at capacity
	if len(s.items) >= s.capacity {
		s.evict()
	}

	elem := s.order.PushFront(key)
	s.items[key] = &entry{
		key:     key,
		value:   resp,
		element: elem,
	}
	return nil
}

// Delete removes a cached entry by key.
func (s *MemoryStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.items[key]; ok {
		s.order.Remove(e.element)
		delete(s.items, key)
	}
	return nil
}

// Clear removes all entries from the cache.
func (s *MemoryStore) Clear() error {
	s.items = make(map[string]*entry)
	s.order.Init()
	return nil
}

func (s *MemoryStore) evict() {
	elem := s.order.Back()
	if elem == nil {
		return
	}
	key := elem.Value.(string)
	delete(s.items, key)
	s.order.Remove(elem)
}

// Size returns the current number of entries in the cache.
func (s *MemoryStore) Size() int {
	return len(s.items)
}

// Stats returns cache statistics for monitoring purposes.
func (s *MemoryStore) Stats() map[string]interface{} {
	return map[string]interface{}{
		"size":     len(s.items),
		"capacity": s.capacity,
	}
}
