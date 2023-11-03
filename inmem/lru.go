package inmem

import (
	"container/list"
	"sync"
)

type (
	// LRU cache (least recently used)
	//
	// The zero value is not ready for use. Refer to [NewLRU] for the factory method.
	LRU[T comparable, K any] struct {
		threshold     int
		evictionQueue *list.List
		hashMap       map[T]*list.Element
		size          int
		mu            *sync.RWMutex
	}
	node[T comparable, K any] struct {
		key   T
		value K
	}
)

// NewLRU creates an in memory cache that applies an LRU policy.
func NewLRU[T comparable, K any](threshold int) *LRU[T, K] {
	return &LRU[T, K]{
		threshold:     threshold,
		evictionQueue: list.New(),
		hashMap:       make(map[T]*list.Element),
		mu:            &sync.RWMutex{},
	}
}

func (l *LRU[T, K]) Get(key T, opts ...Opt) (K, bool) {
	o := getOpts(opts...)

	var unlocked bool
	l.mu.RLock()
	defer func() {
		if !unlocked {
			l.mu.RUnlock()
		}
	}()

	ele, ok := l.hashMap[key]
	if !ok {
		var empty K
		return empty, false
	}

	value := ele.Value.(*node[T, K]).value
	if !o.skipInc {
		l.mu.RUnlock()
		unlocked = true

		l.mu.Lock()
		defer l.mu.Unlock()

		// Check if key is still present between "lock promotion"
		if _, ok := l.hashMap[key]; ok {
			l.evictionQueue.MoveToBack(ele)
		}
	}

	return value, ok
}

func (l *LRU[T, K]) Put(key T, value K, opts ...Opt) bool {
	o := getOpts(opts...)

	l.mu.Lock()
	defer l.mu.Unlock()

	ele, ok := l.hashMap[key]
	if ok {
		ele.Value.(*node[T, K]).value = value
		if !o.skipInc {
			l.evictionQueue.MoveToBack(ele)
		}
	} else {
		l.size += 1
		l.hashMap[key] = l.evictionQueue.PushBack(&node[T, K]{
			key:   key,
			value: value,
		})
	}
	l.unsafeApplyPolicy()

	return ok
}

func (l *LRU[T, K]) Evict(key T) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if ele, ok := l.hashMap[key]; ok {
		l.size -= 1
		l.evictionQueue.Remove(ele)
		return true
	}
	return false
}

func (l *LRU[T, K]) Inc(key T) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if ele, ok := l.hashMap[key]; ok {
		l.evictionQueue.MoveToBack(ele)
		return true
	}
	return false
}

func (l *LRU[T, K]) Values() map[T]K {
	l.mu.RLock()
	defer l.mu.RUnlock()

	values := make(map[T]K, l.size)
	for k, element := range l.hashMap {
		values[k] = element.Value.(*node[T, K]).value
	}
	return values
}

// Not concurrently safe!
func (l *LRU[T, K]) unsafeApplyPolicy() {
	for l.size > l.threshold {
		ele := l.evictionQueue.Front()
		n := ele.Value.(*node[T, K])

		l.size -= 1
		l.evictionQueue.Remove(ele)
		delete(l.hashMap, n.key)
	}
}
