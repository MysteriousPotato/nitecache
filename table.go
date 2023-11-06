package nitecache

import (
	"context"
	"errors"
	"fmt"
	"github.com/MysteriousPotato/nitecache/inmem"
	"strings"
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

type (
	BatchEvictionErrs []batchEvictionErr
	batchEvictionErr  struct {
		keys []string
		err  error
	}
)

type Table[T any] struct {
	name       string
	store      *inmem.Store[string, []byte]
	hotStore   *inmem.Store[string, []byte]
	codec      Codec[T]
	getSF      *singleflight.Group
	evictSF    *singleflight.Group
	procedures map[string]Procedure[T]
	metrics    *metrics
	cache      *Cache
	autofill   bool
}

type getResponse struct {
	value inmem.Item[[]byte]
	hit   bool
}

func (t *Table[T]) Get(ctx context.Context, key string) (T, error) {
	if t.isZero() {
		var empty T
		return empty, ErrCacheDestroyed
	}

	ownerID, err := t.cache.ring.GetOwner(key)
	if err != nil {
		return t.getEmptyValue(), err
	}

	var item inmem.Item[[]byte]
	var hit bool
	if ownerID == t.cache.self.ID {
		item, hit, err = t.getLocally(ctx, key)
	} else {
		client, err := t.cache.getClient(ownerID)
		if err != nil {
			return t.getEmptyValue(), err
		}

		item, hit, err = t.getFromPeer(ctx, key, client)
		if err != nil {
			return t.getEmptyValue(), err
		}
	}
	if err != nil {
		return t.getEmptyValue(), err
	}

	if !hit && !t.autofill {
		return t.getEmptyValue(), ErrKeyNotFound
	}

	var v T
	if err := t.codec.Decode(item.Value, &v); err != nil {
		return t.getEmptyValue(), err
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

	b, err := t.codec.Encode(value)
	if err != nil {
		return err
	}

	if ownerID == t.cache.self.ID {
		if err := t.putLocally(key, t.store.NewItem(b, ttl)); err != nil {
			return err
		}
	} else {
		client, err := t.cache.getClient(ownerID)
		if err != nil {
			return err
		}

		if err := t.putFromPeer(ctx, key, b, ttl, client); err != nil {
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

// EvictAll attempts to remove all entries from the Table for the given keys.
//
// Keys owned by the same client are batched together for efficiency.
//
// After the operation, a BatchEvictionErrs detailing which keys (if any) failed to be evicted can be retrieved when checking the returned error.
// Example:
//
//	if errs, ok := err.(nitecache.BatchEvictionErrs); ok {
//		// Note that keys that AffectedKeys may return keys that were actually evicted successfully.
//		keysThatFailed := errs.AffectedKeys()
//	}
func (t *Table[T]) EvictAll(ctx context.Context, keys []string) error {
	if t.isZero() {
		return ErrCacheDestroyed
	}

	type clientKeys struct {
		client *client
		keys   []string
	}

	var selfKeys []string
	clientKeysMap := map[string]*clientKeys{}
	for _, key := range keys {
		ownerID, err := t.cache.ring.GetOwner(key)
		if err != nil {
			return err
		}

		if ownerID == t.cache.self.ID {
			selfKeys = append(selfKeys, key)
			continue
		}

		if _, ok := clientKeysMap[ownerID]; !ok {
			c, err := t.cache.getClient(ownerID)
			if err != nil {
				return err
			}

			clientKeysMap[ownerID] = &clientKeys{
				client: c,
				keys:   []string{key},
			}
			continue
		}

		clientKeysMap[ownerID].keys = append(clientKeysMap[ownerID].keys, key)
	}

	t.evictAllLocally(selfKeys)

	var errs BatchEvictionErrs
	for _, c := range clientKeysMap {
		if err := t.evictAllFromPeer(ctx, c.keys, c.client); err != nil {
			errs = append(errs, batchEvictionErr{
				keys: c.keys,
				err:  err,
			})
		}
	}

	if errs != nil {
		return errs
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
		return t.getEmptyValue(), err
	}

	var item inmem.Item[[]byte]
	if ownerID == t.cache.self.ID {
		item, err = t.callLocally(ctx, key, function, args)
		if err != nil {
			return t.getEmptyValue(), err
		}
	} else {
		client, err := t.cache.getClient(ownerID)
		if err != nil {
			return t.getEmptyValue(), err
		}

		item, err = t.callFromPeer(ctx, key, function, args, client)
		if err != nil {
			return t.getEmptyValue(), err
		}
	}

	if item.Value == nil {
		return t.getEmptyValue(), nil
	}

	var v T
	if err := t.codec.Decode(item.Value, &v); err != nil {
		return t.getEmptyValue(), err
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
		return t.getEmptyValue(), err
	}

	var item inmem.Item[[]byte]
	var hit bool
	if ownerID == t.cache.self.ID {
		item, hit, err = t.store.Get(context.Background(), key)
		if err != nil {
			return t.getEmptyValue(), err
		}
	} else {
		item, hit, err = t.getFromHotCache(key)
	}
	if err != nil {
		return t.getEmptyValue(), err
	}

	if !hit {
		return t.getEmptyValue(), ErrKeyNotFound
	}

	var v T
	if err := t.codec.Decode(item.Value, &v); err != nil {
		return t.getEmptyValue(), err
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

func (t *Table[T]) getLocally(ctx context.Context, key string) (inmem.Item[[]byte], bool, error) {
	incGet(t.metrics, t.cache.metrics)
	sfRes, err, _ := t.getSF.Do(key, func() (any, error) {
		item, hit, err := t.store.Get(ctx, key)
		if !hit {
			incMiss(t.metrics, t.cache.metrics)
		}
		return getResponse{
			value: item,
			hit:   hit,
		}, err
	})
	res := sfRes.(getResponse)

	return res.value, res.hit, err
}

func (t *Table[T]) putLocally(key string, item inmem.Item[[]byte]) error {
	incPut(t.metrics, t.cache.metrics)
	t.store.Put(key, item)
	return nil
}

func (t *Table[T]) evictLocally(key string) error {
	incEvict(1, t.metrics, t.cache.metrics)
	_, _, _ = t.evictSF.Do(key, func() (any, error) {
		t.store.Evict(key)
		return nil, nil
	})
	return nil
}

func (t *Table[T]) evictAllLocally(keys []string) {
	incEvict(int64(len(keys)), t.metrics, t.cache.metrics)
	t.store.EvictAll(keys)
}

func (t *Table[T]) callLocally(ctx context.Context, key, procedure string, args []byte) (inmem.Item[[]byte], error) {
	incCalls(procedure, t.metrics, t.cache.metrics)

	// Can be access concurrently since no write is possible at this point
	fn, ok := t.procedures[procedure]
	if !ok {
		return inmem.Item[[]byte]{}, ErrRPCNotFound
	}

	return t.store.Update(ctx, key, args, func(ctx context.Context, value []byte, args []byte) ([]byte, time.Duration, error) {
		var v T
		if value != nil {
			if err := t.codec.Decode(value, &v); err != nil {
				return nil, 0, err
			}
		}

		newValue, ttl, err := fn(ctx, v, args)
		if err != nil {
			return nil, 0, err
		}

		b, err := t.codec.Encode(newValue)
		if err != nil {
			return nil, 0, err
		}

		return b, ttl, nil
	})
}

func (t *Table[T]) getFromPeer(ctx context.Context, key string, owner *client) (inmem.Item[[]byte], bool, error) {
	sfRes, err, _ := t.getSF.Do(key, func() (any, error) {
		res, err := owner.Get(ctx, &servicepb.GetRequest{
			Table: t.name,
			Key:   key,
		})
		if err != nil {
			return getResponse{}, err
		}

		item := inmem.Item[[]byte]{
			Expire: time.UnixMicro(res.Item.Expire),
			Value:  res.Item.Value,
		}

		if t.hotStore != nil {
			t.hotStore.Put(key, item)
		}

		return getResponse{
			value: item,
			hit:   res.Hit,
		}, nil
	})
	res := sfRes.(getResponse)

	return res.value, res.hit, err
}

func (t *Table[T]) putFromPeer(ctx context.Context, key string, b []byte, ttl time.Duration, owner *client) error {
	item := t.store.NewItem(b, ttl)

	if _, err := owner.Put(ctx, &servicepb.PutRequest{
		Table: t.name,
		Key:   key,
		Item: &servicepb.Item{
			Expire: item.Expire.UnixMicro(),
			Value:  item.Value,
		},
	}); err != nil {
		return err
	}

	if t.hotStore != nil {
		t.hotStore.Put(key, item)
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
			t.hotStore.Evict(key)
		}

		return nil, nil
	})
	return err
}

func (t *Table[T]) evictAllFromPeer(ctx context.Context, keys []string, owner *client) error {
	if _, err := owner.EvictAll(ctx, &servicepb.EvictAllRequest{
		Table: t.name,
		Keys:  keys,
	}); err != nil {
		return err
	}

	if t.hotStore != nil {
		t.hotStore.EvictAll(keys)
	}
	return nil
}

func (t *Table[T]) callFromPeer(
	ctx context.Context,
	key, procedure string,
	args []byte,
	owner *client,
) (inmem.Item[[]byte], error) {
	res, err := owner.Call(ctx, &servicepb.CallRequest{
		Table:     t.name,
		Key:       key,
		Procedure: procedure,
		Args:      args,
	})
	if err != nil {
		return inmem.Item[[]byte]{}, err
	}

	item := inmem.Item[[]byte]{
		Expire: time.UnixMicro(res.Item.Expire),
		Value:  res.Item.Value,
	}

	if t.hotStore != nil {
		t.hotStore.Put(key, item)
	}

	return item, nil
}

func (t *Table[T]) getFromHotCache(key string) (inmem.Item[[]byte], bool, error) {
	if t.hotStore == nil {
		return inmem.Item[[]byte]{}, false, fmt.Errorf("hot cache not enabled")
	}
	return t.hotStore.Get(context.Background(), key)
}

func (t *Table[T]) tearDown() {
	if t != nil {
		*t = Table[T]{}
	}
}

func (t *Table[T]) isZero() bool {
	return t == nil || t.cache == nil
}

func (t *Table[T]) getEmptyValue() T {
	var v T
	return v
}

func (b BatchEvictionErrs) Error() string {
	var errs []string
	for _, err := range b {
		errs = append(errs, fmt.Sprintf("failed to evict keys %v: %v", err.keys, err.err))
	}
	return strings.Join(errs, ",")
}

// AffectedKeys returns a list of  keys owned by clients who returned an error.
//
// As a result, the list may contain keys that were successfully evicted.
func (b BatchEvictionErrs) AffectedKeys() []string {
	var keys []string
	for _, err := range b {
		keys = append(keys, err.keys...)
	}
	return keys
}
