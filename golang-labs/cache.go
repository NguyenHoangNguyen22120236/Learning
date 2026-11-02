package main

import "sync"

// Cache is an in-memory cache. It must not be copied after first use.
// Its zero value is ready to use. Values containing pointers, slices, or maps
// still require callers to synchronize mutations to the referenced data.
type Cache[K comparable, V any] struct {
	mu     sync.RWMutex
	values map[K]V
}

func NewCache[K comparable, V any]() *Cache[K, V] {
	return &Cache[K, V]{values: make(map[K]V)}
}

func (c *Cache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.values == nil {
		c.values = make(map[K]V)
	}
	c.values[key] = value
}

func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, ok := c.values[key]
	return value, ok
}

func (c *Cache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.values, key)
}
