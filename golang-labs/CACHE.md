# Generic cache lab

`Cache[K comparable, V any]` uses `map[K]V`, preserving compile-time key and value types. Keys must support equality because Go maps require comparable keys. `Set` and `Delete` hold an exclusive mutex lock; `Get` holds a read lock, allowing multiple readers at once. The boolean result distinguishes a missing key from a stored zero value.

```go
cache := NewCache[string, int]()
cache.Set("count", 0)
value, found := cache.Get("count") // 0, true
cache.Delete("count")
value, found = cache.Get("count") // 0, false
```

Run `go test -race ./...` from `golang-labs`. The concurrency test starts 100 readers and 100 writers together, exercises deletion, and verifies final values.

The map lives in one Go process's memory. Each service replica creates its own map, and `sync.RWMutex` coordinates only goroutines sharing this cache instance. Updates do not propagate to other replicas, and restarting the process loses its entries. Shared state requires an external store such as Redis or a database, or an explicit replication mechanism.

The mutex protects cache operations, not later mutations to objects referenced by stored pointers, maps, or slices. Callers must synchronize those mutations themselves.

## Optional TTL extension

The base implementation has no expiration. To add TTL, change the internal map value to an entry containing `value V` and `expiresAt time.Time`, and define whether zero TTL means no expiration or immediate expiration.

With lazy expiration, `Get` treats an expired entry as missing. Removing it requires releasing the read lock, acquiring the write lock, and checking the current entry's expiration again so a concurrent replacement is not deleted. Lazy removal avoids background goroutines, but expired keys that are never read retain memory.

A cleanup worker can periodically delete expired entries under the write lock, reclaiming unread entries too. It needs an explicit shutdown method and ticker cleanup, and large scans can block readers and writers. Reads must still check expiration because cleanup runs periodically. Choose based on memory limits and lifecycle needs.
