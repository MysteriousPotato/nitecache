package nitecache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MysteriousPotato/nitecache/servicepb"
	"golang.org/x/sync/singleflight"
)

var (
	ErrRPCNotFound = errors.New("RPC not found")
	ErrKeyNotFound = errors.New("key not found")
)

// Procedure defines the type used for registering RPCs through [TableBuilder.WithProcedure].
type Procedure[T any] func(ctx context.Context, v T, args []byte) (T, time.Duration, error)

type Table[T any] struct {
	name       string
	store      *store[T]
	hotStore   *store[T]
	getSF      *singleflight.Group
	evictSF    *singleflight.Group
	procedures map[string]Procedure[T]
	metrics    *metrics
	cache      *Cache
}

func (t *Table[T]) Get(ctx context.Context, key string) (T, error) {
	if t.isZero() {
		var empty T
		return empty, ErrCacheDestroyed
	}

	ownerID, err := t.cache.ring.GetOwner(key)
	if err != nil {
		return t.store.getEmptyValue(), err
	}

	var itm item
	if ownerID == t.cache.self.ID {
		itm, err = t.getLocally(key)
	} else {
		client, err := t.cache.getClient(ownerID)
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

func (t *Table[T]) Put(ctx context.Context, key string, value T, ttl time.Duration) error {
	if t.isZero() {
		return ErrCacheDestroyed
	}

	ownerID, err := t.cache.ring.GetOwner(key)
	if err != nil {
		return err
	}

	itm, err := t.store.newItem(key, value, ttl)
	if err != nil {
		return err
	}

	if ownerID == t.cache.self.ID {
		if err := t.putLocally(itm); err != nil {
			return err
		}
	} else {
		client, err := t.cache.getClient(ownerID)
		if err != nil {
			return err
		}

		if err := t.putFromPeer(ctx, itm, client); err != nil {
			return err
		}
	}

	return nil
}

func (t *Table[T]) Evict(ctx context.Context, key string) error {
	if t.isZero() {
		return ErrCacheDestroyed
	}

	ownerID, err := t.cache.ring.GetOwner(key)
	if err != nil {
		return err
	}

	if ownerID == t.cache.self.ID {
		if err := t.evictLocally(key); err != nil {
			return err
		}
	} else {
		client, err := t.cache.getClient(ownerID)
		if err != nil {
			return err
		}

		if err := t.evictFromPeer(ctx, key, client); err != nil {
			return err
		}
	}
	return nil
}

// Call calls an RPC previously registered through [TableBuilder.WithProcedure] on the owner node to update the value for the given key.
//
// Call acquires a lock exclusive to the given key until the RPC has finished executing.
func (t *Table[T]) Call(ctx context.Context, key, function string, args []byte) (T, error) {
	if t.isZero() {
		var empty T
		return empty, ErrCacheDestroyed
	}

	ownerID, err := t.cache.ring.GetOwner(key)
	if err != nil {
		return t.store.getEmptyValue(), err
	}

	var itm item
	if ownerID == t.cache.self.ID {
		itm, err = t.callLocally(ctx, key, function, args)
	} else {
		client, err := t.cache.getClient(ownerID)
		if err != nil {
			return t.store.getEmptyValue(), err
		}

		itm, err = t.callFromPeer(ctx, key, function, args, client)
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
//
// GetHot does not call the getter to autofill cache, does not increment metrics and does not affect the main cache's LFU/LRU (if used).
func (t *Table[T]) GetHot(key string) (T, error) {
	if t.isZero() {
		var empty T
		return empty, ErrCacheDestroyed
	}

	ownerID, err := t.cache.ring.GetOwner(key)
	if err != nil {
		return t.store.getEmptyValue(), err
	}

	var itm item
	if ownerID == t.cache.self.ID {
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

// GetMetrics returns a copy of the current table Metrics. For global cache Metrics, refer to [Cache.GetMetrics]
func (t *Table[T]) GetMetrics() (Metrics, error) {
	if t.isZero() {
		return Metrics{}, ErrCacheDestroyed
	}
	return t.metrics.getCopy(), nil
}

func (t *Table[T]) getLocally(key string) (item, error) {
	incGet(t.metrics, t.cache.metrics)
	sfRes, err, _ := t.getSF.Do(key, func() (any, error) {
		i, hit, err := t.store.get(key)
		if !hit {
			incMiss(t.metrics, t.cache.metrics)
		}
		return i, err
	})

	return sfRes.(item), err
}

func (t *Table[T]) putLocally(itm item) error {
	incPut(t.metrics, t.cache.metrics)
	t.store.put(itm)
	return nil
}

func (t *Table[T]) evictLocally(key string) error {
	incEvict(t.metrics, t.cache.metrics)
	_, _, _ = t.evictSF.Do(key, func() (any, error) {
		t.store.evict(key)
		return nil, nil
	})
	return nil
}

func (t *Table[T]) callLocally(ctx context.Context, key, procedure string, args []byte) (item, error) {
	incCalls(procedure, t.metrics, t.cache.metrics)

	// Can be access concurrently since no write is possible at this point
	fn, ok := t.procedures[procedure]
	if !ok {
		return item{}, ErrRPCNotFound
	}

	return t.store.update(ctx, key, args, fn)
}

func (t *Table[T]) getFromPeer(ctx context.Context, key string, owner *client) (item, error) {
	sfRes, err, _ := t.getSF.Do(key, func() (any, error) {
		res, err := owner.Get(ctx, &servicepb.GetRequest{
			Table: t.name,
			Key:   key,
		})
		if err != nil {
			return item{}, err
		}

		itm := item{
			Expire: time.UnixMicro(res.Item.Expire),
			Value:  res.Item.Value,
			Key:    key,
		}

		if t.hotStore != nil {
			t.hotStore.put(itm)
		}

		return itm, nil
	})

	return sfRes.(item), err
}

func (t *Table[T]) putFromPeer(ctx context.Context, itm item, owner *client) error {
	if _, err := owner.Put(ctx, &servicepb.PutRequest{
		Table: t.name,
		Item: &servicepb.Item{
			Expire: itm.Expire.UnixMicro(),
			Value:  itm.Value,
			Key:    itm.Key,
		},
	}); err != nil {
		return err
	}

	if t.hotStore != nil {
		t.hotStore.put(itm)
	}

	return nil
}

func (t *Table[T]) evictFromPeer(ctx context.Context, key string, owner *client) error {
	_, err, _ := t.evictSF.Do(key, func() (any, error) {
		if _, err := owner.Evict(ctx, &servicepb.EvictRequest{
			Table: t.name,
			Key:   key,
		}); err != nil {
			return nil, err
		}

		if t.hotStore != nil {
			t.hotStore.evict(key)
		}

		return nil, nil
	})
	return err
}

func (t *Table[T]) callFromPeer(
	ctx context.Context,
	key, procedure string,
	args []byte,
	owner *client,
) (item, error) {
	res, err := owner.Call(ctx, &servicepb.CallRequest{
		Table:     t.name,
		Key:       key,
		Procedure: procedure,
		Args:      args,
	})
	if err != nil {
		return item{}, err
	}

	itm := item{
		Expire: time.UnixMicro(res.Item.Expire),
		Value:  res.Item.Value,
		Key:    key,
	}

	if t.hotStore != nil {
		t.hotStore.put(itm)
	}

	return itm, nil
}

func (t *Table[T]) getFromHotCache(key string) (item, error) {
	if t.hotStore == nil {
		return item{}, fmt.Errorf("hot cache not enabled")
	}
	itm, _, _ := t.hotStore.get(key)
	return itm, nil
}

func (t *Table[T]) tearDown() {
	if t != nil {
		*t = Table[T]{}
	}
}

func (t *Table[T]) isZero() bool {
	return t == nil || t.cache == nil
}
