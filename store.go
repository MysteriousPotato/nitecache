package nitecache

import (
	"encoding/json"
	"time"

	"github.com/MysteriousPotato/go-lockable"
)

// Type used for auto cache filling
type Getter[T any] func(key string) (T, time.Duration, error)

type store[T any] struct {
	items          lockable.Map[string, item]
	getter         Getter[T]
	evictionPolicy EvictionPolicy
	closeCh        chan bool
}

type item struct {
	Expire time.Time
	Value  []byte
	Key    string
}

// Eviction policy operations are non-blocking and applied only after store manipulations.
// This may be desirable for lru/noCacheEviction but may cause a key that has just been inserted to be immediatly removed when using lfu
func newStore[T any](evictionPolicy EvictionPolicy, getter Getter[T]) *store[T] {
	s := store[T]{
		items:          lockable.NewMap[string, item](),
		evictionPolicy: NoEvictionPolicy{},
		getter:         getter,
		closeCh:        make(chan bool),
	}
	s.evictionPolicy.setEvictFn(s.items.Delete)

	go func() {
		ticker := time.NewTicker(time.Second * 2)
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

	b, err := json.Marshal(value)
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

	i, hit := s.items.Load(key)
	if s.getter != nil && (!hit || i.isExpired()) {
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

	s.evictionPolicy.push(key, i)

	return i, hit, nil
}

func (s store[T]) put(key string, i item) {
	s.items.LockKey(key)
	defer s.items.UnlockKey(key)

	s.items.Store(key, i)

	s.evictionPolicy.push(key, i)
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

	i, hit := s.items.Load(key)
	if s.getter != nil && (!hit || i.isExpired()) {
		var err error
		i, err = s.unsafeCacheAside(key)
		if err != nil {
			return item{}, false, err
		}
	}

	v, err := s.decode(i)
	if err != nil {
		return item{}, hit, err
	}

	newVal, ttl, err := fn(v)
	if err != nil {
		return item{}, hit, err
	}

	b, err := json.Marshal(newVal)
	if err != nil {
		return item{}, hit, err
	}

	newItem := item{
		Value:  b,
		Expire: time.Now().Add(ttl),
		Key:    key,
	}
	s.items.Store(key, newItem)

	s.evictionPolicy.push(key, i)

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

	s.evictionPolicy.push(key, newItem)

	return newItem, nil
}

func (s store[T]) decode(i item) (T, error) {
	var v T
	if len(i.Value) == 0 {
		return v, nil
	}

	if err := json.Unmarshal(i.Value, &v); err != nil {
		return v, err
	}
	return v, nil
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
