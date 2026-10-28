package main

import (
	"fmt"
	"sync"
)

// Counter starts at zero and is safe for concurrent use.
// A Counter must not be copied after first use; pass it by pointer.
type Counter struct {
	mu sync.Mutex
	n  int64
}

func NewCounter() *Counter {
	return &Counter{}
}

func (c *Counter) Value() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func (c *Counter) Increment() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
}

type Reader interface {
	Value() int64
}

type Writer interface {
	Increment()
}

func main() {
	var _ Reader = (*Counter)(nil)
	var _ Writer = (*Counter)(nil)

	counter := NewCounter()
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			counter.Increment()
		}()
	}
	wg.Wait()
	fmt.Println(counter.Value())
}
