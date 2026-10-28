package main

import (
	"strconv"
	"sync"
	"testing"
)

func TestCounterConcurrent(t *testing.T) {
	var counter Counter
	if got := counter.Get("missing"); got != 0 {
		t.Fatalf("missing key = %d; want 0", got)
	}

	const workers = 1000
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			counter.Increment("shared")
			counter.Increment(strconv.Itoa(id))
			// Exercise reads while other goroutines are writing.
			counter.Get("shared")
		}(i)
	}
	close(start)
	wg.Wait()

	if got := counter.Get("shared"); got != workers {
		t.Errorf("shared count = %d; want %d", got, workers)
	}
	for i := 0; i < workers; i++ {
		if got := counter.Get(strconv.Itoa(i)); got != 1 {
			t.Errorf("key %d count = %d; want 1", i, got)
		}
	}
}
