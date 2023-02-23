package nitecache

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/MysteriousPotato/nitecache/servicepb"
	"github.com/pkg/errors"
	"golang.org/x/sync/singleflight"
)

var (
	ErrFunctionNotFound = errors.New("table function not found")
	ErrTableKeyNotFound = errors.New("Table key not found")
)

// type used for registering functions through [TableBuilder.WithFunction]
type Function[T any] func(v T, args []byte) (T, time.Duration, error)

type Table[T any] struct {
	name            string
	store           *store[T]
	hotStore        *store[T]
	hotCacheEnabled bool
	getSF           *singleflight.Group
	evictSF         *singleflight.Group
	functions       map[string]Function[T]
	functionsMu     *sync.RWMutex
	metrics         *Metrics
}

type TableBuilder[T any] struct {
	name            string
	evictionPolicy  EvictionPolicy
	hotCacheEnabled bool
	functions       map[string]Function[T]
	getter          Getter[T]
}

func NewTable[T any](name string) *TableBuilder[T] {
	return &TableBuilder[T]{
		name:      name,
		functions: map[string]Function[T]{},
	}
}

// Adds a callback function used for auto cache filling
func (t *TableBuilder[T]) WithGetter(fn Getter[T]) *TableBuilder[T] {
	t.getter = fn
	return t
}

// see [EvictionPolicy]
func (t *TableBuilder[T]) WithEvictionPolicy(policy EvictionPolicy) *TableBuilder[T] {
	t.evictionPolicy = policy
	return t
}

// Registers a function that can be called using [Table.Execute]
func (t *TableBuilder[T]) WithFunction(name string, function Function[T]) *TableBuilder[T] {
	t.functions[name] = function
	return t
}

// If hot cache is enable, a new cache will be populated with values gotten from other peers that can be accessed only through [Table.GetHot].
func (t *TableBuilder[T]) WithHotCache() *TableBuilder[T] {
	t.hotCacheEnabled = true
	return t
}

func (b *TableBuilder[T]) Build() *Table[T] {
	if b.evictionPolicy == nil {
		b.evictionPolicy = NoEvictionPolicy{}
	}

	t := &Table[T]{
		name:            b.name,
		store:           newStore(b.evictionPolicy, b.getter),
		hotStore:        newStore[T](b.evictionPolicy, nil),
		hotCacheEnabled: b.hotCacheEnabled,
		getSF:           &singleflight.Group{},
		evictSF:         &singleflight.Group{},
		functions:       b.functions,
		functionsMu:     &sync.RWMutex{},
		metrics:         newMetrics(),
	}

	tables[b.name] = t

	return t
}

func (t Table[T]) Get(ctx context.Context, key string) (T, error) {
	owner, client, err := getOwner(key)
	if err != nil {
		return t.getEmptyValue(), err
	}

	var i item
	if owner.ID == pself.ID {
		i, err = t.getLocally(key)
	} else {
		i, err = t.getFromPeer(ctx, key, client)
	}
	if err != nil {
		return t.getEmptyValue(), err
	}

	if i.isZero() {
		return t.getEmptyValue(), ErrTableKeyNotFound
	}

	v, err := t.store.decode(i)
	if err != nil {
		return t.getEmptyValue(), err
	}

	return v, nil
}

func (t Table[T]) Put(ctx context.Context, key string, value T, ttl time.Duration) error {
	i, err := t.store.newItem(key, value, ttl)
	if err != nil {
		return err
	}

	owner, client, err := getOwner(key)
	if err != nil {
		return err
	}

	if owner.ID == pself.ID {
		t.putLocally(key, i)
	} else {
		if err := t.putFromPeer(ctx, i, client); err != nil {
			return err
		}
	}

	return nil
}

func (t Table[T]) Evict(ctx context.Context, key string) error {
	owner, client, err := getOwner(key)
	if err != nil {
		return err
	}

	if owner.ID == pself.ID {
		t.evictLocally(key)
	} else {
		if err := t.evictFromPeer(ctx, key, client); err != nil {
			return err
		}
	}
	return nil
}

// Executes a function previously registered through [TableBuilder.WithFunction] to atomically update the value for a given key
func (t Table[T]) Execute(ctx context.Context, key, function string, args []byte) (T, error) {
	owner, client, err := getOwner(key)
	if err != nil {
		return t.getEmptyValue(), err
	}

	var i item
	if owner.ID == pself.ID {
		i, err = t.executeLocally(key, function, args)
	} else {
		i, err = t.executeFromPeer(ctx, key, function, args, client)
	}
	if err != nil {
		return t.getEmptyValue(), err
	}

	v, err := t.store.decode(i)
	if err != nil {
		return t.getEmptyValue(), err
	}

	return v, err
}

// looks up local cache if the current node is the owner, otherwise looks up  hotcache
func (t Table[T]) GetHot(key string) (T, error) {
	owner, _, err := getOwner(key)
	if err != nil {
		return t.getEmptyValue(), err
	}

	var i item
	if owner.ID == pself.ID {
		i, err = t.getLocally(key)
	} else {
		i, err = t.getFromHotCache(key)
	}
	if err != nil {
		return t.getEmptyValue(), err
	}

	if i.isZero() {
		return t.getEmptyValue(), ErrTableKeyNotFound
	}

	v, err := t.store.decode(i)
	if err != nil {
		return t.getEmptyValue(), err
	}

	return v, nil
}

// Can safely be called from a  goroutine, returns a copy of the current table Metrics.
// For global cache Metrics, refer to [GetMetrics]
func (t Table[T]) GetMetrics() Metrics {
	return t.metrics.getCopy()
}

func (t Table[T]) getLocally(key string) (item, error) {
	incGet(t.metrics, metrics)
	sfRes, err, _ := t.getSF.Do(key, func() (any, error) {
		i, hit, err := t.store.get(key)
		if !hit {
			incMiss(t.metrics, metrics)
		}

		return i, err
	})
	i := sfRes.(item)

	return i, err
}

func (t Table[T]) putLocally(key string, i item) {
	incPut(t.metrics, metrics)
	t.store.put(key, i)
}

func (t Table[T]) evictLocally(key string) {
	incEvict(t.metrics, metrics)
	t.evictSF.Do(key, func() (any, error) {
		t.store.evict(key)
		return nil, nil
	})
}

func (t Table[T]) executeLocally(key, function string, args []byte) (item, error) {
	incExecute(function, t.metrics, metrics)
	e, ok := t.functions[function]
	if !ok {
		return item{}, ErrFunctionNotFound
	}

	i, hit, err := t.store.update(key, func(value T) (T, time.Duration, error) {
		return e(value, args)
	})
	if !hit {
		incMiss(t.metrics, metrics)
	}

	return i, err
}

func (t Table[T]) getFromPeer(ctx context.Context, key string, owner *client) (item, error) {
	sfRes, err, _ := t.getSF.Do(key, func() (any, error) {
		out, err := owner.Get(ctx, &servicepb.GetRequest{
			Table: t.name,
			Key:   key,
		})
		if err != nil {
			return item{}, err
		}

		i := item{
			Expire: time.UnixMicro(out.Item.Expire),
			Value:  out.Item.Value,
			Key:    out.Item.Key,
		}

		if t.hotCacheEnabled {
			t.hotStore.put(i.Key, i)
		}

		return i, nil
	})
	i := sfRes.(item)

	return i, err
}

func (t Table[T]) putFromPeer(ctx context.Context, i item, owner *client) error {
	if _, err := owner.Put(ctx, &servicepb.PutRequest{
		Table: t.name,
		Value: &servicepb.Item{
			Expire: i.Expire.UnixMicro(),
			Value:  i.Value,
			Key:    i.Key,
		},
	}); err != nil {
		return err
	}

	if t.hotCacheEnabled {
		t.hotStore.put(i.Key, i)
	}

	return nil
}

func (t Table[T]) evictFromPeer(ctx context.Context, key string, owner *client) error {
	_, err, _ := t.evictSF.Do(key, func() (any, error) {
		if _, err := owner.Evict(ctx, &servicepb.EvictRequest{
			Table: t.name,
			Key:   key,
		}); err != nil {
			return nil, err
		}

		if t.hotCacheEnabled {
			t.hotStore.evict(key)
		}

		return nil, nil
	})
	return err
}

func (t Table[T]) executeFromPeer(ctx context.Context, key, function string, args []byte, owner *client) (item, error) {
	out, err := owner.Execute(ctx, &servicepb.ExecuteRequest{
		Table:    t.name,
		Key:      key,
		Function: function,
		Args:     args,
	})
	if err != nil {
		return item{}, err
	}

	i := item{
		Expire: time.UnixMicro(out.Value.Expire),
		Value:  out.Value.Value,
		Key:    key,
	}

	if t.hotCacheEnabled {
		t.hotStore.put(key, i)
	}

	return i, nil
}

func (t Table[T]) getFromHotCache(key string) (item, error) {
	i, _, err := t.hotStore.get(key)
	return i, err
}

func (t Table[T]) decode(b []byte) (item, error) {
	var i item
	if err := json.Unmarshal(b, &i); err != nil {
		return i, err
	}
	return i, nil
}

func (t Table[T]) getEmptyValue() T {
	var v T
	return v
}
