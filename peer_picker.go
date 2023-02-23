package nitecache

import (
	"crypto/md5"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type peerPicker interface {
	get(key string) *Peer
	set(peers []Peer)
}

type consistentHashingPeerPicker struct {
	hashMap    map[int]*Peer
	keys       []int
	mu         *sync.RWMutex
	peersCount int
	self       *Peer
	vNodes     int
}

func newConsistentHasingPeerPicker(self *Peer, vNodes int) peerPicker {
	return &consistentHashingPeerPicker{
		mu:      &sync.RWMutex{},
		hashMap: map[int]*Peer{},
		self:    self,
		vNodes:  vNodes,
	}
}

func (h *consistentHashingPeerPicker) set(peers []Peer) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.peersCount = len(peers)
	if h.peersCount == 1 {
		return
	}

	for n, p := range peers {
		for i := 0; i < h.vNodes; i++ {
			keyHash := md5.Sum([]byte(strconv.Itoa(i) + p.ID))

			hash := fnv.New64()
			hash.Write(keyHash[:])
			sum := int(hash.Sum64())

			h.hashMap[sum] = &peers[n]
			h.keys = append(h.keys, sum)
		}
	}

	sort.Ints(h.keys)
}

func (h consistentHashingPeerPicker) get(key string) *Peer {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.peersCount == 1 {
		return h.self
	}

	hash := fnv.New64()
	hash.Write([]byte(key))
	sum := int(hash.Sum64())
	i := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= sum
	})

	if i == len(h.keys) {
		i = 0
	}

	return h.hashMap[h.keys[i]]
}
