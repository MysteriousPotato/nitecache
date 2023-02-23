package nitecache

import (
	"container/list"
	"sync"
)

// Currently supports lru, lfu and no eviction policy
//
// For lru and lfu implementations, threshold represents the memory of cached items at which the policy will start eviction.
//
// Note that these are arbitrary values that do not reflect how much memory is actually used by a table, but only the memory used the encoded values stored
type EvictionPolicy interface {
	push(key string, i item)
	evict(key string)
	setEvictFn(onEvict func(key string))
}

type NoEvictionPolicy struct{}

func (n NoEvictionPolicy) push(key string, i item)             {}
func (n NoEvictionPolicy) evict(key string)                    {}
func (n NoEvictionPolicy) setEvictFn(onEvict func(key string)) {}

// see [EvictionPolicy]
type Lru struct {
	treshold      int64
	evictionQueue *list.List
	hashMap       map[string]*list.Element
	size          int64
	mu            *sync.Mutex
	onEvict       func(key string)
}

type lruValue struct {
	key  string
	size int64
}

// see [EvictionPolicy]
func NewLruPolicy(threshold int64) *Lru {
	return &Lru{
		treshold:      threshold,
		evictionQueue: list.New(),
		hashMap:       make(map[string]*list.Element),
		mu:            &sync.Mutex{},
	}
}

func (l *Lru) setEvictFn(onEvict func(key string)) {
	l.onEvict = onEvict
}

func (l *Lru) push(key string, i item) {
	l.mu.Lock()
	defer l.mu.Unlock()

	ele, ok := l.hashMap[key]
	if ok {
		currSize := ele.Value.(*lruValue).size
		newSize := int64(len(i.Value))
		l.size += newSize - currSize

		ele.Value = &lruValue{
			key:  key,
			size: newSize,
		}
		l.evictionQueue.MoveToBack(ele)
	} else {
		v := &lruValue{
			key:  key,
			size: int64(len(i.Value)),
		}
		l.hashMap[key] = l.evictionQueue.PushBack(v)
		l.size += int64(len(i.Value))
	}

	l.unsafeEvictOldest()
}

func (l *Lru) evict(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	ele, ok := l.hashMap[key]
	if ok {
		size := ele.Value.(*lruValue).size
		l.size -= size
		l.evictionQueue.Remove(ele)
	}
}

func (l *Lru) unsafeEvictOldest() {
	for l.size > l.treshold {
		ele := l.evictionQueue.Front()
		value := ele.Value.(*lruValue)

		l.size -= value.size
		l.onEvict(value.key)
		l.evictionQueue.Remove(ele)
		delete(l.hashMap, value.key)
	}
}

// see [EvictionPolicy]
type Lfu struct {
	treshold int64
	freqList *list.List
	hashMap  map[string]*lfuEntry
	size     int64
	mu       *sync.Mutex
	onEvict  func(key string)
}

type lfuEntry struct {
	key    string
	size   int64
	parent *list.Element
}

type lfuNode struct {
	count   int
	entries map[string]*lfuEntry
}

// see [EvictionPolicy]
func NewLfuPolicy(threshold int64) *Lfu {
	return &Lfu{
		treshold: threshold,
		freqList: list.New(),
		hashMap:  make(map[string]*lfuEntry),
		mu:       &sync.Mutex{},
	}
}

func (l *Lfu) setEvictFn(onEvict func(key string)) {
	l.onEvict = onEvict
}

func (l *Lfu) push(key string, i item) {
	l.mu.Lock()
	defer l.mu.Unlock()

	//Upsert the entry and update cache size
	entry, ok := l.hashMap[key]
	if ok {
		prevSize := entry.size
		entry.size = int64(len(i.Value))
		l.size += entry.size - prevSize
	} else {
		entry = &lfuEntry{
			size: int64(len(i.Value)),
			key:  key,
		}
		l.hashMap[key] = entry
		l.size += entry.size
	}

	if entry.parent == nil {
		//create a new freqList node if necessary && Add the new entry to the freqList node
		first := l.freqList.Front()
		if first == nil || first.Value.(*lfuNode).count != 0 {
			entry.parent = l.freqList.PushFront(&lfuNode{
				count: 0,
				entries: map[string]*lfuEntry{
					key: entry,
				},
			})
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
			entry.parent = l.freqList.InsertAfter(&lfuNode{
				count: nextCount,
				entries: map[string]*lfuEntry{
					key: entry,
				},
			}, entry.parent)
		}
		l.unsafeRemoveFreqEntry(prevNode, entry)
	}
	l.unsafeEvictLeastUsed()
}

func (l *Lfu) evict(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	value, ok := l.hashMap[key]
	if ok {
		delete(l.hashMap, value.key)
		l.unsafeRemoveFreqEntry(value.parent, value)
	}
}

// Not concurrently safe!
func (l *Lfu) unsafeEvictLeastUsed() {
	for l.size > l.treshold {
		node := l.freqList.Front()
		nodeValue := node.Value.(*lfuNode)

		var entry *lfuEntry
		for _, e := range nodeValue.entries {
			entry = e
			break
		}

		l.size -= entry.size
		l.onEvict(entry.key)
		delete(l.hashMap, entry.key)
		l.unsafeRemoveFreqEntry(node, entry)
	}
}

// Not concurrently safe!
// Removes a specific entry from a given freqList node
func (l *Lfu) unsafeRemoveFreqEntry(node *list.Element, entry *lfuEntry) {
	nodeValue := node.Value.(*lfuNode)

	delete(nodeValue.entries, entry.key)
	if len(nodeValue.entries) == 0 {
		l.freqList.Remove(node)
	}
}
