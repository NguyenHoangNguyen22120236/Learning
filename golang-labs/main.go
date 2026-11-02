package main

import "fmt"

func main() {
	cache := NewCache[string, int]()
	cache.Set("count", 0)
	value, found := cache.Get("count")
	fmt.Printf("Stored zero: value=%d, found=%t\n", value, found)
	cache.Delete("count")
	value, found = cache.Get("count")
	fmt.Printf("After deletion: value=%d, found=%t\n", value, found)
}
