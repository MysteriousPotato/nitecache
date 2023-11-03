package nitecache

import (
	"context"
	"github.com/MysteriousPotato/nitecache/inmem"
	"time"

	"github.com/MysteriousPotato/go-lockable"
)

// Getter Type used for auto cache filling
type Getter[T any] func(key string) (T, time.Duration, error)

type (
	storeOpts[T any] struct {
		getter  Getter[T]
		storage Storage
		codec   Codec[T]
	}
	store[T any] struct {
		lock     lockable.Lockable[string]
		getter   Getter[T]
		internal Storage
		codec    Codec[T]
	}
	item struct {
		Expire time.Time
		Value  []byte
		Key    string
	}
)

type Storage interface {
	Put(key string, value item, opt ...inmem.Opt) bool
	Evict(key string) bool
	Get(key string, opt ...inmem.Opt) (item, bool)
}

func LFU(threshold int) Storage {
	return inmem.NewLFU[string, item](threshold)
}

func LRU(threshold int) Storage {
	return inmem.NewLRU[string, item](threshold)
}

func newStore[T any](opts storeOpts[T]) *store[T] {
	if opts.codec == nil {
		var v T
		anyV := any(v)
		if _, isByteSlice := anyV.([]byte); isByteSlice {
			opts.codec = any(StringCodec[[]byte]{}).(Codec[T])
		} else if _, isString := anyV.(string); isString {
			opts.codec = any(StringCodec[string]{}).(Codec[T])
		} else {
			opts.codec = &JsonCodec[T]{}
		}
	}

	var storage Storage
	if opts.storage == nil {
		storage = inmem.NewCache[string, item]()
	} else {
		storage = opts.storage
	}

	s := store[T]{
		lock:     lockable.New[string](),
		getter:   opts.getter,
		codec:    opts.codec,
		internal: storage,
	}

	return &s
}

func (s store[T]) get(key string) (item, bool, error) {
	var unlocked bool
	s.lock.RLockKey(key)
	defer func() {
		if !unlocked {
			s.lock.RUnlockKey(key)
		}
	}()

	itm, hit := s.internal.Get(key)
	if s.getter != nil && (!hit || itm.isExpired()) {
		s.lock.RUnlockKey(key)
		unlocked = true

		s.lock.LockKey(key)
		defer s.lock.UnlockKey(key)

		itm, err := s.unsafeCacheAside(key)
		if err != nil {
			return itm, false, err
		}

		return itm, false, nil
	}
	return itm, hit, nil
}

func (s store[T]) put(itm item) {
	s.lock.LockKey(itm.Key)
	defer s.lock.UnlockKey(itm.Key)

	s.internal.Put(itm.Key, itm)
}

func (s store[T]) evict(key string) {
	s.lock.LockKey(key)
	defer s.lock.UnlockKey(key)

	s.internal.Evict(key)
}

func (s store[T]) update(
	ctx context.Context,
	key string,
	args []byte,
	fn func(context.Context, T, []byte) (T, time.Duration, error),
) (item, error) {
	s.lock.LockKey(key)
	defer s.lock.UnlockKey(key)

	oldItem, ok := s.internal.Get(key, inmem.SkipInc(true))
	var skipInc bool
	if !ok && s.getter != nil {
		skipInc = true
		var err error
		if oldItem, err = s.unsafeCacheAside(key); err != nil {
			return item{}, err
		}
	}

	v, err := s.decode(oldItem)
	if err != nil {
		return item{}, err
	}

	newValue, ttl, err := fn(ctx, v, args)
	if err != nil {
		return item{}, err
	}

	newItem, err := s.newItem(key, newValue, ttl)
	if err != nil {
		return item{}, err
	}

	s.internal.Put(newItem.Key, newItem, inmem.SkipInc(skipInc))

	return newItem, nil
}

// Make sure to lock the key before using this
func (s store[T]) unsafeCacheAside(key string) (item, error) {
	v, ttl, err := s.getter(key)
	if err != nil {
		return item{}, err
	}

	newItem, err := s.newItem(key, v, ttl)
	if err != nil {
		return item{}, err
	}

	s.internal.Put(key, newItem, inmem.SkipInc(true))

	return newItem, nil
}

func (s store[T]) newItem(key string, value T, ttl time.Duration) (item, error) {
	var expire time.Time
	if ttl != 0 {
		expire = time.Now().Add(ttl)
	}

	b, err := s.codec.Encode(value)
	if err != nil {
		return item{}, err
	}

	return item{
		Expire: expire,
		Value:  b,
		Key:    key,
	}, nil
}

func (s store[T]) decode(itm item) (T, error) {
	var v T
	if len(itm.Value) == 0 {
		return v, nil
	}

	if err := s.codec.Decode(itm.Value, &v); err != nil {
		return v, err
	}
	return v, nil
}

func (s store[T]) getEmptyValue() T {
	var v T
	return v
}

func (i item) isExpired() bool {
	return !i.Expire.IsZero() && i.Expire.Before(time.Now())
}

func (i item) isZero() bool {
	return i.Key == ""
}
