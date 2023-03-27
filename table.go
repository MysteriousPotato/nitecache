package nitecache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/MysteriousPotato/nitecache/servicepb"
	"golang.org/x/sync/singleflight"
)

var (
	ErrFunctionNotFound = errors.New("table function not found")
	ErrKeyNotFound      = errors.New("key not found")
)

// Function type used for registering functions through [TableBuilder.WithFunction]
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
	cache           *Cache
}

func (t Table[T]) Get(ctx context.Context, key string) (T, error) {
	owner, err := t.cache.ring.getOwner(key)
	if err != nil {
		return t.store.getEmptyValue(), err
	}

	var itm item
	if owner.ID == t.cache.selfID {
		itm, err = t.getLocally(key)
	} else {
		client, err := t.cache.getClient(owner)
		if err != nil {
			return t.store.getEmptyValue(), err
		}

		itm, err = t.getFromPeer(ctx, key, client)
		if err != nil {
			return t.store.getEmptyValue(), err
		}
	}
	if err != nil {
		return t.store.getEmptyValue(), err
	}

	if itm.isZero() {
		return t.store.getEmptyValue(), ErrKeyNotFound
	}

	v, err := t.store.decode(itm)
	if err != nil {
		return t.store.getEmptyValue(), err
	}

	return v, nil
}

func (t Table[T]) Put(ctx context.Context, key string, value T, ttl time.Duration) error {
	m, err := t.cache.ring.getOwner(key)
	if err != nil {
		return err
	}

	itm, err := t.store.newItem(key, value, ttl)
	if err != nil {
		return err
	}

	if m.ID == t.cache.selfID {
		t.putLocally(itm)
	} else {
		client, err := t.cache.getClient(m)
		if err != nil {
			return err
		}

		if err := t.putFromPeer(ctx, itm, client); err != nil {
			return err
		}
	}

	return nil
}

func (t Table[T]) Evict(ctx context.Context, key string) error {
	m, err := t.cache.ring.getOwner(key)
	if err != nil {
		return err
	}

	if m.ID == t.cache.selfID {
		t.evictLocally(key)
	} else {
		client, err := t.cache.getClient(m)
		if err != nil {
			return err
		}

		if err := t.evictFromPeer(ctx, key, client); err != nil {
			return err
		}
	}
	return nil
}

// Execute Executes a function previously registered through [TableBuilder.WithFunction] to atomically update the value for a given key
func (t Table[T]) Execute(ctx context.Context, key, function string, args []byte) (T, error) {
	owner, err := t.cache.ring.getOwner(key)
	if err != nil {
		return t.store.getEmptyValue(), err
	}

	var itm item
	if owner.ID == t.cache.selfID {
		itm, err = t.executeLocally(key, function, args)
	} else {
		client, err := t.cache.getClient(owner)
		if err != nil {
			return t.store.getEmptyValue(), err
		}

		itm, err = t.executeFromPeer(ctx, key, function, args, client)
		if err != nil {
			return t.store.getEmptyValue(), err
		}
	}
	if err != nil {
		return t.store.getEmptyValue(), err
	}

	v, err := t.store.decode(itm)
	if err != nil {
		return t.store.getEmptyValue(), err
	}

	return v, err
}

// GetHot looks up local cache if the current node is the owner, otherwise looks up  hot cache.
// GetHot does not call the getter to autofill cache, it is not token into account into metrics.
func (t Table[T]) GetHot(key string) (T, error) {
	owner, err := t.cache.ring.getOwner(key)
	if err != nil {
		return t.store.getEmptyValue(), err
	}

	var itm item
	if owner.ID == t.cache.selfID {
		itm, _, err = t.store.get(key)
		if err != nil {
			return t.store.getEmptyValue(), err
		}
	} else {
		itm, err = t.getFromHotCache(key)
	}
	if err != nil {
		return t.store.getEmptyValue(), err
	}

	if itm.isZero() {
		return t.store.getEmptyValue(), ErrKeyNotFound
	}

	v, err := t.store.decode(itm)
	if err != nil {
		return t.store.getEmptyValue(), err
	}

	return v, nil
}

// GetMetrics Can safely be called from a  goroutine, returns a copy of the current table Metrics.
// For global cache Metrics, refer to [GetMetrics]
func (t Table[T]) GetMetrics() Metrics {
	return t.metrics.getCopy()
}

func (t Table[T]) getLocally(key string) (item, error) {
	incGet(t.metrics, t.cache.metrics)
	sfRes, err, _ := t.getSF.Do(
		key, func() (any, error) {
			i, hit, err := t.store.get(key)
			if !hit {
				incMiss(t.metrics, t.cache.metrics)
			}
			return i, err
		},
	)

	return sfRes.(item), err
}

func (t Table[T]) putLocally(itm item) {
	incPut(t.metrics, t.cache.metrics)
	t.store.put(itm)
}

func (t Table[T]) evictLocally(key string) {
	incEvict(t.metrics, t.cache.metrics)
	_, _, _ = t.evictSF.Do(
		key, func() (any, error) {
			t.store.evict(key)
			return nil, nil
		},
	)
}

func (t Table[T]) executeLocally(key, function string, args []byte) (item, error) {
	incExecute(function, t.metrics, t.cache.metrics)
	fn, ok := t.functions[function]
	if !ok {
		return item{}, ErrFunctionNotFound
	}

	t.store.items.LockKey(key)
	defer t.store.items.UnlockKey(key)

	oldItem, _ := t.store.items.Load(key)
	v, err := t.store.decode(oldItem)
	if err != nil {
		return item{}, err
	}

	newValue, ttl, err := fn(v, args)
	if err != nil {
		return item{}, err
	}

	newItem, err := t.store.newItem(key, newValue, ttl)
	if err != nil {
		return item{}, err
	}

	t.store.items.Store(newItem.Key, newItem)

	return newItem, err
}

func (t Table[T]) getFromPeer(ctx context.Context, key string, owner client) (item, error) {
	sfRes, err, _ := t.getSF.Do(
		key, func() (any, error) {
			out, err := owner.Get(
				ctx, &servicepb.GetRequest{
					Table: t.name,
					Key:   key,
				},
			)
			if err != nil {
				return item{}, err
			}

			itm := item{
				Expire: time.UnixMicro(out.Item.Expire),
				Value:  out.Item.Value,
				Key:    out.Item.Key,
			}

			if t.hotCacheEnabled {
				t.hotStore.put(itm)
			}

			return itm, nil
		},
	)

	return sfRes.(item), err
}

func (t Table[T]) putFromPeer(ctx context.Context, itm item, owner client) error {
	if _, err := owner.Put(
		ctx, &servicepb.PutRequest{
			Table: t.name,
			Item: &servicepb.Item{
				Expire: itm.Expire.UnixMicro(),
				Value:  itm.Value,
				Key:    itm.Key,
			},
		},
	); err != nil {
		return err
	}

	if t.hotCacheEnabled {
		t.hotStore.put(itm)
	}

	return nil
}

func (t Table[T]) evictFromPeer(ctx context.Context, key string, owner client) error {
	_, err, _ := t.evictSF.Do(
		key, func() (any, error) {
			if _, err := owner.Evict(
				ctx, &servicepb.EvictRequest{
					Table: t.name,
					Key:   key,
				},
			); err != nil {
				return nil, err
			}

			if t.hotCacheEnabled {
				t.hotStore.evict(key)
			}

			return nil, nil
		},
	)
	return err
}

func (t Table[T]) executeFromPeer(
	ctx context.Context,
	key, function string,
	args []byte,
	owner client,
) (item, error) {
	out, err := owner.Execute(
		ctx, &servicepb.ExecuteRequest{
			Table:    t.name,
			Key:      key,
			Function: function,
			Args:     args,
		},
	)
	if err != nil {
		return item{}, err
	}

	itm := item{
		Expire: time.UnixMicro(out.Item.Expire),
		Value:  out.Item.Value,
		Key:    key,
	}

	if t.hotCacheEnabled {
		t.hotStore.put(itm)
	}

	return itm, nil
}

func (t Table[T]) getFromHotCache(key string) (item, error) {
	if !t.hotCacheEnabled {
		return item{}, fmt.Errorf("hot cache not enabled")
	}
	itm, _, _ := t.hotStore.get(key)
	return itm, nil
}

// TearDown Call this whenever a table is not needed anymore
//
// It will properly free the [Table] from [Cache] and close all goroutines
func (t Table[T]) TearDown() {
	t.store.closeCh <- true
	t.hotStore.closeCh <- true
	delete(t.cache.tables, t.name)
}
