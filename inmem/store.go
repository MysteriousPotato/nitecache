package inmem

import (
	"context"
	"time"

	"github.com/MysteriousPotato/go-lockable"
)

// Getter Type used for auto cache filling
type Getter[K comparable, V any] func(key K) (V, time.Duration, error)

type (
	StoreOpt[K comparable, V any] func(*Store[K, V])
	Store[K comparable, V any]    struct {
		lock     lockable.Lockable[K]
		getter   Getter[K, V]
		internal Storage[K, V]
	}
	Item[T any] struct {
		Expire time.Time
		Value  T
	}
)

type Storage[K comparable, V any] interface {
	Put(key K, value Item[V], opt ...Opt) bool
	Evict(key K) bool
	Get(key K, opt ...Opt) (Item[V], bool)
}

func WithStorage[K comparable, V any](storage Storage[K, V]) StoreOpt[K, V] {
	return func(s *Store[K, V]) {
		s.internal = storage
	}
}

func WithGetter[K comparable, V any](getter Getter[K, V]) StoreOpt[K, V] {
	return func(s *Store[K, V]) {
		s.getter = getter
	}
}

func NewStore[K comparable, V any](opts ...StoreOpt[K, V]) *Store[K, V] {
	s := &Store[K, V]{
		lock: lockable.New[K](),
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.internal == nil {
		s.internal = NewCache[K, Item[V]]()
	}

	return s
}

func (s Store[K, V]) Get(key K) (Item[V], bool, error) {
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

func (s Store[K, V]) Put(key K, item Item[V]) {
	s.lock.LockKey(key)
	defer s.lock.UnlockKey(key)

	s.internal.Put(key, item)
}

func (s Store[K, V]) Evict(key K) {
	s.lock.LockKey(key)
	defer s.lock.UnlockKey(key)

	s.internal.Evict(key)
}

func (s Store[K, V]) Update(
	ctx context.Context,
	key K,
	args []byte,
	fn func(context.Context, V, []byte) (V, time.Duration, error),
) (Item[V], error) {
	s.lock.LockKey(key)
	defer s.lock.UnlockKey(key)

	oldItem, ok := s.internal.Get(key, SkipInc(true))
	var skipInc bool
	if !ok && s.getter != nil {
		skipInc = true
		var err error
		if oldItem, err = s.unsafeCacheAside(key); err != nil {
			return Item[V]{}, err
		}
	}

	newValue, ttl, err := fn(ctx, oldItem.Value, args)
	if err != nil {
		return Item[V]{}, err
	}

	newItem := s.NewItem(newValue, ttl)
	s.internal.Put(key, newItem, SkipInc(skipInc))

	return newItem, nil
}

func (s Store[K, V]) NewItem(value V, ttl time.Duration) Item[V] {
	var exp time.Time
	if ttl != 0 {
		exp = time.Now().Add(ttl)
	}

	return Item[V]{
		Expire: exp,
		Value:  value,
	}
}

// Make sure to lock the key before using this
func (s Store[K, V]) unsafeCacheAside(key K) (Item[V], error) {
	v, ttl, err := s.getter(key)
	if err != nil {
		return Item[V]{}, err
	}

	newItem := s.NewItem(v, ttl)
	s.internal.Put(key, newItem, SkipInc(true))

	return newItem, nil
}

func (s Store[K, V]) getEmptyValue() V {
	var v V
	return v
}

func (i Item[V]) isExpired() bool {
	return !i.Expire.IsZero() && i.Expire.Before(time.Now())
}
