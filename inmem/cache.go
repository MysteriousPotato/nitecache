package inmem

import "sync"

// Cache is essentially a wrapper around map[T]K that support concurrent safety
type Cache[T comparable, K any] struct {
	internal map[T]K
	mu       *sync.RWMutex
}

func NewCache[T comparable, K any]() *Cache[T, K] {
	return &Cache[T, K]{
		internal: map[T]K{},
		mu:       &sync.RWMutex{},
	}
}

func (c *Cache[T, K]) Get(key T, _ ...Opt) (K, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	v, ok := c.internal[key]
	return v, ok
}

func (c *Cache[T, K]) Put(key T, value K, _ ...Opt) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.internal[key]
	c.internal[key] = value

	return ok
}

func (c *Cache[T, K]) Evict(key T) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.internal[key]; ok {
		delete(c.internal, key)
		return true
	}
	return false
}

func (c *Cache[T, K]) Inc(_ string) bool { return false }
