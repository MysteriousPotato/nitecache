package nitecache

import (
	"golang.org/x/sync/singleflight"
	"sync"
)

type TableBuilder[T any] struct {
	name            string
	evictionPolicy  EvictionPolicy
	hotCacheEnabled bool
	functions       map[string]Function[T]
	getter          Getter[T]
	codec           Codec[T]
}

func NewTable[T any](name string) *TableBuilder[T] {
	return &TableBuilder[T]{
		name:      name,
		functions: map[string]Function[T]{},
	}
}

// WithGetter Adds a callback function used for auto cache filling
func (tb *TableBuilder[T]) WithGetter(fn Getter[T]) *TableBuilder[T] {
	tb.getter = fn
	return tb
}

// WithEvictionPolicy see [EvictionPolicy]
func (tb *TableBuilder[T]) WithEvictionPolicy(policy EvictionPolicy) *TableBuilder[T] {
	tb.evictionPolicy = policy
	return tb
}

// WithFunction Registers a function that can be called using [Table.Execute]
func (tb *TableBuilder[T]) WithFunction(name string, function Function[T]) *TableBuilder[T] {
	tb.functions[name] = function
	return tb
}

// WithHotCache If hot cache is enable, a new cache will be populated with values gotten from other peers that can be accessed only through [Table.GetHot].
// The owner of the hot cache is responsible for keeping it up to date.
func (tb *TableBuilder[T]) WithHotCache() *TableBuilder[T] {
	tb.hotCacheEnabled = true
	return tb
}

// WithCodec overrides the default encoding/decoding behavior. Defaults to stdlib [json.Marshal]/[json.Unmarshal]
func (tb *TableBuilder[T]) WithCodec(codec Codec[T]) *TableBuilder[T] {
	tb.codec = codec
	return tb
}

func (tb *TableBuilder[T]) Build(c *Cache) *Table[T] {
	if tb.evictionPolicy == nil {
		tb.evictionPolicy = NoEvictionPolicy{}
	}

	t := &Table[T]{
		name:            tb.name,
		getSF:           &singleflight.Group{},
		evictSF:         &singleflight.Group{},
		functions:       tb.functions,
		functionsMu:     &sync.RWMutex{},
		metrics:         newMetrics(),
		hotCacheEnabled: tb.hotCacheEnabled,
		hotStore: newStore(
			storeOpts[T]{
				evictionPolicy: tb.evictionPolicy,
				codec:          tb.codec,
			},
		),
		store: newStore(
			storeOpts[T]{
				evictionPolicy: tb.evictionPolicy,
				getter:         tb.getter,
				codec:          tb.codec,
			},
		),
		cache: c,
	}

	c.tables[tb.name] = t
	return t
}
