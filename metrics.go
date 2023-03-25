package nitecache

import (
	"sync"
	"sync/atomic"
)

// Metrics are limited to the scope of the owner nodes.
//
// For example, if node-1 queries node-2, metrics will be registered on node-2 only.
type Metrics struct {
	Miss    int64
	Get     int64
	Put     int64
	Evict   int64
	Execute map[string]int64
	mu      *sync.Mutex
}

func (m Metrics) getCopy() Metrics {
	m.mu.Lock()
	defer m.mu.Unlock()

	return Metrics{
		Miss:    m.Miss,
		Get:     m.Get,
		Put:     m.Put,
		Evict:   m.Evict,
		Execute: m.Execute,
	}
}

func newMetrics() *Metrics {
	return &Metrics{
		Execute: make(map[string]int64),
		mu:      &sync.Mutex{},
	}
}

func incMiss(metrics ...*Metrics) {
	for _, m := range metrics {
		atomic.AddInt64(&m.Miss, 1)
	}
}

func incGet(metrics ...*Metrics) {
	for _, m := range metrics {
		atomic.AddInt64(&m.Get, 1)
	}
}

func incPut(metrics ...*Metrics) {
	for _, m := range metrics {
		atomic.AddInt64(&m.Put, 1)
	}
}

func incEvict(metrics ...*Metrics) {
	for _, m := range metrics {
		atomic.AddInt64(&m.Evict, 1)
	}
}

func incExecute(function string, metrics ...*Metrics) {
	inc := func(m *Metrics) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.Execute[function]++
	}
	for _, m := range metrics {
		inc(m)
	}
}
