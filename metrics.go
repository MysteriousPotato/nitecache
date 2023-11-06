package nitecache

import (
	"maps"
	"sync"
	"sync/atomic"
)

type (
	// Metrics are limited to the scope of the owner nodes.
	//
	// For example, if node-1 queries node-2, metrics will be registered on node-2 only.
	Metrics struct {
		Miss  int64
		Get   int64
		Put   int64
		Evict int64
		Call  map[string]int64
	}
	metrics struct {
		Miss  atomic.Int64
		Get   atomic.Int64
		Put   atomic.Int64
		Evict atomic.Int64
		Call  map[string]int64
		mu    *sync.RWMutex
	}
)

func (m *metrics) getCopy() Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return Metrics{
		Miss:  m.Miss.Load(),
		Get:   m.Get.Load(),
		Put:   m.Put.Load(),
		Evict: m.Evict.Load(),
		Call:  maps.Clone(m.Call),
	}
}

func newMetrics() *metrics {
	return &metrics{
		Call: make(map[string]int64),
		mu:   &sync.RWMutex{},
	}
}

func incMiss(ms ...*metrics) {
	for _, m := range ms {
		m.Miss.Add(1)
	}
}

func incGet(ms ...*metrics) {
	for _, m := range ms {
		m.Get.Add(1)
	}
}

func incPut(ms ...*metrics) {
	for _, m := range ms {
		m.Put.Add(1)
	}
}

func incEvict(delta int64, ms ...*metrics) {
	for _, m := range ms {
		m.Evict.Add(delta)
	}
}

func incCalls(procedure string, ms ...*metrics) {
	for _, m := range ms {
		incCall(procedure, m)
	}
}

func incCall(procedure string, m *metrics) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Call[procedure]++
}
