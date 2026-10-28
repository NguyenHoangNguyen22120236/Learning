package main

import (
	"sync"
	"testing"
)

func TestCounterConcurrent(t *testing.T) {
	counter := NewCounter()
	if got := counter.Value(); got != 0 {
		t.Fatalf("initial value = %d; want 0", got)
	}

	const workers = 1000
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			counter.Increment()
			counter.Value() // Exercise reads while other goroutines write.
		}()
	}
	close(start)
	wg.Wait()

	if got := counter.Value(); got != workers {
		t.Fatalf("final value = %d; want %d", got, workers)
	}
}
