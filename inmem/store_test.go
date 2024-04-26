package inmem_test

import (
	"context"
	"github.com/MysteriousPotato/nitecache/inmem"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestStore(t *testing.T) {
	s := inmem.NewStore(inmem.WithGetter[string, string](func(_ context.Context, _ string) (string, time.Duration, error) {
		return "empty", 0, nil
	}))

	ops := []struct {
		op  string
		val string
		key string
	}{
		{op: "get", key: "1"},
		{op: "put", key: "1", val: "test"},
		{op: "get", key: "1"},
		{op: "evict", key: "1"},
		{op: "get", key: "1"},
		{op: "put", key: "1", val: "test"},
		{op: "get", key: "1"},
		{op: "update", key: "1"},
		{op: "update", key: "2"},
	}
	expected := []string{"empty", "test", "empty", "test", "test test", "empty empty"}
	var got []string

	for _, op := range ops {
		if op.op == "get" {
			item, _, err := s.Get(context.Background(), op.key)
			if err != nil {
				t.Fatal(err)
			}

			got = append(got, item.Value)
		}
		if op.op == "put" {
			s.Put(op.key, s.NewItem(op.val, 0))
		}
		if op.op == "evict" {
			s.Evict(op.key)
		}
		if op.op == "update" {
			item, err := s.Update(
				context.Background(),
				op.key,
				nil,
				func(ctx context.Context, value string, args []byte) (string, time.Duration, error) {
					return strings.Join([]string{value, value}, " "), 0, nil
				},
			)
			if err != nil {
				t.Fatal(err)
			}

			got = append(got, item.Value)
		}
	}

	if !reflect.DeepEqual(expected, got) {
		t.Fatalf("expected: %v\ngot:%v", expected, got)
	}
}

func TestStoreTTL(t *testing.T) {
	s := inmem.NewStore[string, string]()

	ops := []struct {
		op      string
		expired bool
		ttl     time.Duration
		wait    time.Duration
	}{
		{op: "put", ttl: time.Second},
		{op: "get"},
		{op: "put", ttl: time.Millisecond, wait: time.Millisecond},
		{op: "get", expired: true},
	}

	for _, op := range ops {
		if op.op == "get" {
			item, _, err := s.Get(context.Background(), "1")
			if err != nil {
				t.Fatal(err)
			}

			isExpired := item.IsExpired()
			if isExpired != op.expired {
				t.Errorf("expected expired %v, got %v", op.expired, isExpired)
			}
		}
		if op.op == "put" {
			s.Put("1", s.NewItem("test", op.ttl))
			time.Sleep(op.wait)
		}
		if op.op == "evict" {
			s.Evict("1")
		}
	}
}
