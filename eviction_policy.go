package nitecache

import (
	"container/list"
	"sync"
)

// EvictionPolicy Currently supports lru, lfu and no eviction policy
//
// For lru and lfu implementations, threshold represents the number of items at which the policy will start eviction.
type EvictionPolicy interface {
	push(key string)
	evict(key string)
	apply()
	setEvictFn(onEvict func(key string))
}

type NoEvictionPolicy struct{}

func (n NoEvictionPolicy) push(_ string)                 {}
func (n NoEvictionPolicy) evict(_ string)                {}
func (n NoEvictionPolicy) setEvictFn(_ func(key string)) {}
func (n NoEvictionPolicy) apply()                        {}

type lru struct {
	threshold     int64
	evictionQueue *list.List
	hashMap       map[string]*list.Element
	size          int64
	mu            *sync.Mutex
	onEvict       func(key string)
}

// NewLruPolicy see [EvictionPolicy]
func NewLruPolicy(threshold int64) EvictionPolicy {
	return &lru{
		threshold:     threshold,
		evictionQueue: list.New(),
		hashMap:       make(map[string]*list.Element),
		mu:            &sync.Mutex{},
	}
}

func (l *lru) setEvictFn(onEvict func(key string)) {
	l.onEvict = onEvict
}

func (l *lru) push(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	ele, ok := l.hashMap[key]
	if ok {
		ele.Value = &key
		l.evictionQueue.MoveToBack(ele)
	} else {
		v := &key
		l.hashMap[key] = l.evictionQueue.PushBack(v)
		l.size += 1
	}
}

func (l *lru) evict(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	ele, ok := l.hashMap[key]
	if ok {
		l.size -= 1
		l.evictionQueue.Remove(ele)
	}
}

func (l *lru) apply() {
	l.mu.Lock()
	defer l.mu.Unlock()

	for l.size > l.threshold {
		ele := l.evictionQueue.Front()
		key := *ele.Value.(*string)

		l.size -= 1
		l.onEvict(key)
		l.evictionQueue.Remove(ele)
		delete(l.hashMap, key)
	}
}

// see [EvictionPolicy]
type lfu struct {
	threshold int
	size      int
	freqList  *list.List
	hashMap   map[string]*lfuEntry
	mu        *sync.Mutex
	onEvict   func(key string)
}

type lfuEntry struct {
	key    string
	parent *list.Element
}

type lfuNode struct {
	count   int
	entries map[string]*lfuEntry
}

// NewLfuPolicy see [EvictionPolicy]
func NewLfuPolicy(threshold int) EvictionPolicy {
	return &lfu{
		threshold: threshold,
		freqList:  list.New(),
		hashMap:   make(map[string]*lfuEntry),
		mu:        &sync.Mutex{},
	}
}

func (l *lfu) setEvictFn(onEvict func(key string)) {
	l.onEvict = onEvict
}

func (l *lfu) push(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	//Upsert the entry and update cache size
	entry, ok := l.hashMap[key]
	if !ok {
		entry = &lfuEntry{key: key}
		l.hashMap[key] = entry
		l.size += 1
	}

	if entry.parent == nil {
		//create a new freqList node if necessary && Add the new entry to the freqList node
		first := l.freqList.Front()
		if first == nil || first.Value.(*lfuNode).count != 0 {
			entry.parent = l.freqList.PushFront(
				&lfuNode{
					count: 0,
					entries: map[string]*lfuEntry{
						key: entry,
					},
				},
			)
		} else {
			first.Value.(*lfuNode).entries[key] = l.hashMap[key]
			entry.parent = first
		}
	} else {
		//Create a new freqList node if necessary && move the entry to the next freqList node
		prevNode := entry.parent
		nextCount := prevNode.Value.(*lfuNode).count + 1

		if next := entry.parent.Next(); next != nil && next.Value.(*lfuNode).count == nextCount {
			next.Value.(*lfuNode).entries[key] = entry
			entry.parent = next
		} else {
			entry.parent = l.freqList.InsertAfter(
				&lfuNode{
					count: nextCount,
					entries: map[string]*lfuEntry{
						key: entry,
					},
				}, entry.parent,
			)
		}
		l.unsafeRemoveFreqEntry(prevNode, entry)
	}
}

func (l *lfu) evict(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	value, ok := l.hashMap[key]
	if ok {
		delete(l.hashMap, value.key)
		l.unsafeRemoveFreqEntry(value.parent, value)
	}
}

func (l *lfu) apply() {
	l.mu.Lock()
	defer l.mu.Unlock()

	for l.size > l.threshold {
		node := l.freqList.Front()
		nodeValue := node.Value.(*lfuNode)

		var entry *lfuEntry
		for _, e := range nodeValue.entries {
			entry = e
			break
		}

		l.size -= 1
		l.onEvict(entry.key)
		delete(l.hashMap, entry.key)
		l.unsafeRemoveFreqEntry(node, entry)
	}
}

// Not concurrently safe!
// Removes a specific entry from a given freqList node
func (l *lfu) unsafeRemoveFreqEntry(node *list.Element, entry *lfuEntry) {
	nodeValue := node.Value.(*lfuNode)

	delete(nodeValue.entries, entry.key)
	if len(nodeValue.entries) == 0 {
		l.freqList.Remove(node)
	}
}
