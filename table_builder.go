package nitecache

import (
	"context"
	"github.com/MysteriousPotato/nitecache/inmem"
	"golang.org/x/sync/singleflight"
	"time"
)

type TableBuilder[T any] struct {
	name       string
	storage    inmem.Storage[string, []byte]
	hotStorage inmem.Storage[string, []byte]
	procedures map[string]Procedure[T]
	getter     inmem.Getter[string, T]
	codec      Codec[T]
}

func NewTable[T any](name string) *TableBuilder[T] {
	return &TableBuilder[T]{
		name:       name,
		procedures: map[string]Procedure[T]{},
	}
}

func LFU(threshold int) inmem.Storage[string, []byte] {
	return inmem.NewLFU[string, inmem.Item[[]byte]](threshold)
}
func LRU(threshold int) inmem.Storage[string, []byte] {
	return inmem.NewLRU[string, inmem.Item[[]byte]](threshold)
}

// WithGetter sets the auto cache filling function.
func (tb *TableBuilder[T]) WithGetter(fn inmem.Getter[string, T]) *TableBuilder[T] {
	tb.getter = fn
	return tb
}

// WithStorage specifies how to store values.
//
// Must be one of [LFU], [LRU] or nil.
//
// if nil, the table will always grow unless keys are explicitly evicted.
func (tb *TableBuilder[T]) WithStorage(storage inmem.Storage[string, []byte]) *TableBuilder[T] {
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
func (tb *TableBuilder[T]) WithHotCache(storage inmem.Storage[string, []byte]) *TableBuilder[T] {
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
		autofill:   tb.getter != nil,
		codec:      tb.codec,
		cache:      c,
	}

	if t.codec == nil {
		var v T
		anyV := any(v)
		if _, isByteSlice := anyV.([]byte); isByteSlice {
			t.codec = any(StringCodec[[]byte]{}).(Codec[T])
		} else if _, isString := anyV.(string); isString {
			t.codec = any(StringCodec[string]{}).(Codec[T])
		} else {
			t.codec = &JsonCodec[T]{}
		}
	}

	storageOpts := []inmem.StoreOpt[string, []byte]{inmem.WithStorage(tb.storage)}
	if tb.getter != nil {
		storageOpts = append(storageOpts, inmem.WithGetter(func(ctx context.Context, key string) ([]byte, time.Duration, error) {
			v, ttl, err := tb.getter(ctx, key)
			if err != nil {
				return nil, 0, err
			}

			b, err := t.codec.Encode(v)
			if err != nil {
				return nil, 0, err
			}

			return b, ttl, nil
		}))
	}
	t.store = inmem.NewStore[string, []byte](storageOpts...)

	if tb.hotStorage != nil {
		t.hotStore = inmem.NewStore[string, []byte](inmem.WithStorage(tb.hotStorage))
	}

	c.tablesMu.Lock()
	defer c.tablesMu.Unlock()

	c.tables[tb.name] = t
	return t
}
