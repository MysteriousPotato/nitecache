package inmem

import (
	"container/list"
	"sync"
)

type (
	// LFU cache (least frequently used)
	//
	// The zero value is not ready for use. Refer to [NewLFU] for the factory method.
	LFU[T comparable, K any] struct {
		threshold int
		size      int
		freqList  *list.List
		hashMap   map[T]*lfuEntry[T, K]
		mu        *sync.RWMutex
	}
	lfuEntry[T comparable, K any] struct {
		key     T
		value   K
		nodeKey *list.Element
		parent  *list.Element
	}
	lfuNode[T any] struct {
		count int
		keys  *list.List
	}
)

// NewLFU creates an in memory cache that applies an LFU policy.
//
// When the cache must eviction keys and multiple keys have the same usage count, [LFU] fallbacks to an LRU policy to determine which key to evict.
func NewLFU[T comparable, K any](threshold int) *LFU[T, K] {
	return &LFU[T, K]{
		threshold: threshold,
		freqList:  list.New(),
		hashMap:   make(map[T]*lfuEntry[T, K]),
		mu:        &sync.RWMutex{},
	}
}

func (l *LFU[T, K]) Get(key T, opts ...Opt) (K, bool) {
	o := getOpts(opts...)

	var unlocked bool
	l.mu.RLock()
	defer func() {
		if !unlocked {
			l.mu.RUnlock()
		}
	}()

	//Upsert the entry and update cache size
	entry, ok := l.hashMap[key]
	if !ok {
		var empty K
		return empty, false
	}

	value := entry.value
	if !o.skipInc {
		l.mu.RUnlock()
		unlocked = true

		l.mu.Lock()
		defer l.mu.Unlock()

		// Check if key is still present between "lock promotion"
		if _, ok := l.hashMap[key]; ok {
			l.unsafeUpdateCount(entry, false)
		}
	}
	return value, ok
}

func (l *LFU[T, K]) Put(key T, value K, opts ...Opt) bool {
	o := getOpts(opts...)

	l.mu.Lock()
	defer l.mu.Unlock()

	// Upsert the entry and update cache size
	entry, ok := l.hashMap[key]
	if ok {
		entry.value = value
	} else {
		entry = &lfuEntry[T, K]{key: key, value: value}
		l.hashMap[key] = entry
		l.size += 1
	}
	l.unsafeApplyPolicy()

	if !ok || !o.skipInc {
		l.unsafeUpdateCount(entry, !ok)
	}
	return ok
}

func (l *LFU[T, K]) Evict(key T) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if value, ok := l.hashMap[key]; ok {
		delete(l.hashMap, value.key)
		l.unsafeRemoveFreqEntry(value.parent, value.nodeKey)
		return true
	}
	return false
}

func (l *LFU[T, K]) Inc(key T) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if entry, ok := l.hashMap[key]; ok {
		l.unsafeUpdateCount(entry, false)
		return true
	}
	return false
}

func (l *LFU[T, K]) Values() map[T]K {
	l.mu.RLock()
	defer l.mu.RUnlock()

	values := make(map[T]K, l.size)
	for k, v := range l.hashMap {
		values[k] = v.value
	}
	return values
}

func (l *LFU[T, K]) unsafeUpdateCount(entry *lfuEntry[T, K], isNewEntry bool) {
	var currentNode, prevNode *list.Element
	var nextCount int
	if isNewEntry {
		currentNode = l.freqList.Front()
	} else {
		currentNode = entry.parent.Next()
		prevNode = entry.parent
		nextCount = prevNode.Value.(*lfuNode[T]).count + 1
	}

	if currentNode == nil || currentNode.Value.(*lfuNode[T]).count != nextCount {
		parentNodeEntries := list.New()
		entry.nodeKey = parentNodeEntries.PushFront(entry.key)
		entry.parent = l.freqList.PushFront(&lfuNode[T]{
			keys:  parentNodeEntries,
			count: nextCount,
		})
	} else {
		entry.nodeKey = currentNode.Value.(*lfuNode[T]).keys.PushFront(entry.key)
		entry.parent = currentNode
	}

	if prevNode != nil {
		l.unsafeRemoveFreqEntry(prevNode, entry.nodeKey)
	}
}

// Not concurrently safe!
func (l *LFU[T, K]) unsafeApplyPolicy() {
	for l.size > l.threshold {
		node := l.freqList.Front()
		nodeValue := node.Value.(*lfuNode[T])
		entry := nodeValue.keys.Back()

		l.size -= 1
		delete(l.hashMap, entry.Value.(T))

		l.unsafeRemoveFreqEntry(node, entry)
	}
}

// Not concurrently safe!
// Removes a specific entry from a given freqList node
func (l *LFU[T, K]) unsafeRemoveFreqEntry(node *list.Element, entry *list.Element) {
	nodeValue := node.Value.(*lfuNode[T])
	nodeValue.keys.Remove(entry)
	if nodeValue.keys.Front() == nil {
		l.freqList.Remove(node)
	}
}
