package main

import (
	"sync"
	"testing"
)

func TestCache(t *testing.T) {
	c := NewCache[string, int]()
	if value, ok := c.Get("missing"); ok || value != 0 {
		t.Fatalf("missing key: got (%d, %v)", value, ok)
	}
	c.Set("count", 0)
	if value, ok := c.Get("count"); !ok || value != 0 {
		t.Fatalf("stored zero: got (%d, %v)", value, ok)
	}
	c.Set("count", 42)
	if value, ok := c.Get("count"); !ok || value != 42 {
		t.Fatalf("overwrite: got (%d, %v)", value, ok)
	}
	c.Delete("count")
	c.Delete("missing")
	if _, ok := c.Get("count"); ok {
		t.Fatal("deleted key still exists")
	}
}

func TestCacheStructValuesAndZeroValue(t *testing.T) {
	type key struct{ ID int }
	type value struct{ Name string }
	var c Cache[key, value]
	want := value{Name: "Alice"}
	c.Set(key{1}, want)
	if got, ok := c.Get(key{1}); !ok || got != want {
		t.Fatalf("Get() = (%v, %v); want (%v, true)", got, ok, want)
	}
}

func TestCacheConcurrentReadersAndWriters(t *testing.T) {
	c := NewCache[int, int]()
	const workers = 100
	const iterations = 1000
	var wg sync.WaitGroup
	start := make(chan struct{})
	for id := 0; id < workers; id++ {
		wg.Add(2)
		go func(id int) {
			defer wg.Done()
			<-start
			for n := 0; n < iterations; n++ {
				c.Set(id, n)
				c.Delete(id)
			}
			c.Set(id, iterations)
		}(id)
		go func(id int) {
			defer wg.Done()
			<-start
			for n := 0; n < iterations; n++ {
				if value, ok := c.Get(id); ok && (value < 0 || value > iterations) {
					t.Errorf("unexpected value %d", value)
					return
				}
			}
		}(id)
	}
	close(start)
	wg.Wait()
	for id := 0; id < workers; id++ {
		if value, ok := c.Get(id); !ok || value != iterations {
			t.Errorf("key %d: got (%d, %v); want (%d, true)", id, value, ok, iterations)
		}
	}
}
