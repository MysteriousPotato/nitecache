package nitecache

import (
	"time"

	"github.com/MysteriousPotato/go-lockable"
)

// Getter Type used for auto cache filling
type Getter[T any] func(key string) (T, time.Duration, error)

type storeOpts[T any] struct {
	getter         Getter[T]
	evictionPolicy EvictionPolicy
	codec          Codec[T]
}

type store[T any] struct {
	items          lockable.Map[string, item]
	getter         Getter[T]
	evictionPolicy EvictionPolicy
	closeCh        chan bool
	codec          Codec[T]
}

type item struct {
	Expire time.Time
	Value  []byte
	Key    string
}

func newStore[T any](opts storeOpts[T]) *store[T] {
	if opts.evictionPolicy == nil {
		opts.evictionPolicy = NoEvictionPolicy{}
	}
	if opts.codec == nil {
		opts.codec = &jsonCodec[T]{}
	}
	s := store[T]{
		items:          lockable.NewMap[string, item](),
		evictionPolicy: opts.evictionPolicy,
		getter:         opts.getter,
		codec:          opts.codec,
		closeCh:        make(chan bool),
	}
	s.evictionPolicy.setEvictFn(s.items.Delete)

	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			select {
			case <-s.closeCh:
				return
			default:
				s.evictionPolicy.apply()
			}
		}
	}()

	return &s
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

func (s store[T]) get(key string) (item, bool, error) {
	var unlocked bool
	s.items.RLockKey(key)
	defer func() {
		if !unlocked {
			s.items.RUnlockKey(key)
		}
	}()

	itm, hit := s.items.Load(key)
	if s.getter != nil && (!hit || itm.isExpired()) {
		s.items.RUnlockKey(key)
		unlocked = true

		s.items.LockKey(key)
		defer s.items.UnlockKey(key)

		item, err := s.unsafeCacheAside(key)
		if err != nil {
			return item, false, err
		}

		return item, false, nil
	}

	s.evictionPolicy.push(key)

	return itm, hit, nil
}

func (s store[T]) put(itm item) {
	s.items.LockKey(itm.Key)
	defer s.items.UnlockKey(itm.Key)

	s.items.Store(itm.Key, itm)
	s.evictionPolicy.push(itm.Key)
}

func (s store[T]) evict(key string) {
	s.items.LockKey(key)
	defer s.items.UnlockKey(key)

	s.items.Delete(key)
	s.evictionPolicy.evict(key)
}

func (s store[T]) update(key string, fn func(value T) (T, time.Duration, error)) (item, bool, error) {
	s.items.LockKey(key)
	defer s.items.UnlockKey(key)

	itm, hit := s.items.Load(key)
	if s.getter != nil && (!hit || itm.isExpired()) {
		var err error
		itm, err = s.unsafeCacheAside(key)
		if err != nil {
			return item{}, false, err
		}
	}

	v, err := s.decode(itm)
	if err != nil {
		return item{}, hit, err
	}

	newVal, ttl, err := fn(v)
	if err != nil {
		return item{}, hit, err
	}

	b, err := s.codec.Encode(newVal)
	if err != nil {
		return item{}, hit, err
	}

	newItem := item{
		Value:  b,
		Expire: time.Now().Add(ttl),
		Key:    key,
	}
	s.items.Store(key, newItem)

	s.evictionPolicy.push(key)

	return newItem, hit, nil
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

	s.items.Store(key, newItem)

	s.evictionPolicy.push(key)

	return newItem, nil
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

func (s store[T]) close() {
	s.closeCh <- true
}

func (i item) isExpired() bool {
	return !i.Expire.IsZero() && i.Expire.Before(time.Now())
}

func (i item) isZero() bool {
	return i.Key == ""
}
