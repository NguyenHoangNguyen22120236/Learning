package main

import "sync"

// Counter is safe for concurrent use. Its zero value is ready to use.
// A Counter must not be copied after first use.
type Counter struct {
	mu     sync.RWMutex
	counts map[string]int
}

func (c *Counter) Increment(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.counts == nil {
		c.counts = make(map[string]int)
	}
	c.counts[key]++
}

func (c *Counter) Get(key string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.counts[key]
}
