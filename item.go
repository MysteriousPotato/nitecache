package nitecache

import (
	"encoding/json"
	"time"
)

type item struct {
	Expire time.Time
	Value  []byte
	Key    string
}

func (i item) isExpired() bool {
	return !i.Expire.IsZero() && i.Expire.Before(time.Now())
}

func (i item) isZero() bool {
	return i.Key == ""
}

func newItem[T any](key string, value T, ttl time.Duration) (item, error) {
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
