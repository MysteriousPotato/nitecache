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
	s := inmem.NewStore(inmem.WithGetter[string, string](func(_ string) (string, time.Duration, error) {
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
			item, _, err := s.Get(op.key)
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
