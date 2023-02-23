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

type Function[T any] func(v T, args []byte) (T, time.Duration, error)

type Table[T any] struct {
	name        string
	store       *store[T]
	getSF       *singleflight.Group
	evictSF     *singleflight.Group
	functions   map[string]Function[T]
	functionsMu *sync.RWMutex
	metrics     *Metrics
}

type TableBuilder[T any] struct {
	name           string
	evictionPolicy EvictionPolicy
	functions      map[string]Function[T]
	getter         Getter[T]
}

func NewTable[T any](name string) *TableBuilder[T] {
	return &TableBuilder[T]{
		name:      name,
		functions: map[string]Function[T]{},
	}
}

func (t *TableBuilder[T]) WithGetter(fn Getter[T]) *TableBuilder[T] {
	t.getter = fn
	return t
}

func (t *TableBuilder[T]) WithEvictionPolicy(policy EvictionPolicy) *TableBuilder[T] {
	t.evictionPolicy = policy
	return t
}

func (t *TableBuilder[T]) WithFunction(name string, function Function[T]) *TableBuilder[T] {
	t.functions[name] = function
	return t
}

func (b *TableBuilder[T]) Build() *Table[T] {
	if b.evictionPolicy == nil {
		b.evictionPolicy = NoEvictionPolicy{}
	}

	t := &Table[T]{
		name:        b.name,
		store:       newStore(b.evictionPolicy, b.getter),
		getSF:       &singleflight.Group{},
		evictSF:     &singleflight.Group{},
		functions:   b.functions,
		functionsMu: &sync.RWMutex{},
		metrics:     newMetrics(),
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
	item, err := t.store.newItem(key, value, ttl)
	if err != nil {
		return err
	}

	owner, client, err := getOwner(key)
	if err != nil {
		return err
	}

	if owner.ID == pself.ID {
		t.putLocally(key, item)
	} else {
		if err := t.putFromPeer(ctx, key, item, client); err != nil {
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

func (t Table[T]) GetMetrics() Metrics {
	return t.metrics.getCopy()
}

func (t Table[T]) getLocally(key string) (item, error) {
	incGet(t.metrics, metrics)
	sfRes, err, _ := t.getSF.Do(key, func() (any, error) {
		item, hit, err := t.store.get(key)
		if !hit {
			incMiss(t.metrics, metrics)
		}

		return item, err
	})
	i := sfRes.(item)

	return i, err
}

func (t Table[T]) putLocally(key string, value item) {
	incPut(t.metrics, metrics)
	t.store.put(key, value)
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

		return item{
			Expire: time.UnixMicro(out.Item.Expire),
			Value:  out.Item.Value,
			Key:    out.Item.Key,
		}, nil
	})
	i := sfRes.(item)

	return i, err
}

func (t Table[T]) putFromPeer(ctx context.Context, key string, value item, owner *client) error {
	_, err := owner.Put(ctx, &servicepb.PutRequest{
		Table: t.name,
		Value: &servicepb.Item{
			Expire: value.Expire.UnixMicro(),
			Value:  value.Value,
			Key:    key,
		},
	})
	return err
}

func (t Table[T]) evictFromPeer(ctx context.Context, key string, owner *client) error {
	_, err, _ := t.evictSF.Do(key, func() (any, error) {
		_, err := owner.Evict(ctx, &servicepb.EvictRequest{
			Table: t.name,
			Key:   key,
		})

		return nil, err
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

	return item{
		Expire: time.UnixMicro(out.Value.Expire),
		Value:  out.Value.Value,
	}, nil
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
