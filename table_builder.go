package nitecache

import (
	"golang.org/x/sync/singleflight"
)

type TableBuilder[T any] struct {
	name       string
	storage    Storage
	hotStorage Storage
	procedures map[string]Procedure[T]
	getter     Getter[T]
	codec      Codec[T]
}

func NewTable[T any](name string) *TableBuilder[T] {
	return &TableBuilder[T]{
		name:       name,
		procedures: map[string]Procedure[T]{},
	}
}

// WithGetter sets the auto cache filling function.
func (tb *TableBuilder[T]) WithGetter(fn Getter[T]) *TableBuilder[T] {
	tb.getter = fn
	return tb
}

// WithStorage specifies how to store values.
//
// Must be one of [LFU], [LRU] or nil.
//
// if nil, the table will always grow unless keys are explicitly evicted.
func (tb *TableBuilder[T]) WithStorage(storage Storage) *TableBuilder[T] {
	tb.storage = storage
	return tb
}

// WithProcedure Registers an RPC that can be called using [Table.Call].
func (tb *TableBuilder[T]) WithProcedure(name string, function Procedure[T]) *TableBuilder[T] {
	tb.procedures[name] = function
	return tb
}

// WithHotCache enables hot cache.
//
// If hot cache is enabled, a new cache will be populated with values gotten from other peers that can be accessed only through [Table.GetHot].
//
// The owner of the hot cache is responsible for keeping it up to date (i.e. calls to [Table.Put] and [Table.Evict] won't update hot cache of other peers).
func (tb *TableBuilder[T]) WithHotCache(storage Storage) *TableBuilder[T] {
	tb.hotStorage = storage
	return tb
}

// WithCodec overrides the default encoding/decoding behavior.
//
// Defaults to [BytesCodec] for []byte tables and [JsonCodec] for any other types.
// See [Codec] for custom implementation.
func (tb *TableBuilder[T]) WithCodec(codec Codec[T]) *TableBuilder[T] {
	tb.codec = codec
	return tb
}

func (tb *TableBuilder[T]) Build(c *Cache) *Table[T] {
	t := &Table[T]{
		name:       tb.name,
		getSF:      &singleflight.Group{},
		evictSF:    &singleflight.Group{},
		procedures: tb.procedures,
		metrics:    newMetrics(),
		store: newStore(storeOpts[T]{
			storage: tb.storage,
			getter:  tb.getter,
			codec:   tb.codec,
		}),
		cache: c,
	}

	if tb.hotStorage != nil {
		t.hotStore = newStore(storeOpts[T]{
			storage: tb.hotStorage,
			codec:   tb.codec,
		})
	}

	c.tablesMu.Lock()
	defer c.tablesMu.Unlock()

	c.tables[tb.name] = t
	return t
}
