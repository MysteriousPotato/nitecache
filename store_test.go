package nitecache

import (
	"reflect"
	"testing"
	"time"
)

func TestStore(t *testing.T) {
	s := newStore(
		storeOpts[string]{
			getter: func(key string) (string, time.Duration, error) {
				return "empty", 0, nil
			},
		},
	)

	ops := []struct {
		op  string
		val string
		key string
	}{
		{op: "get", key: "1"},
		{op: "put", key: "1"},
		{op: "get", key: "1"},
		{op: "evict", key: "1"},
		{op: "get", key: "1"},
		{op: "put", key: "1"},
		{op: "get", key: "1"},
		{op: "update", key: "1"},
		{op: "update", key: "2"},
	}
	expected := []string{"empty", "1", "empty", "1", "11", "emptyempty"}
	var got []string

	for _, op := range ops {
		if op.op == "get" {
			item, _, err := s.get(op.key)
			if err != nil {
				t.Fatal(err)
			}

			v, err := s.decode(item)
			if err != nil {
				t.Fatal(err)
			}

			got = append(got, v)
		}
		if op.op == "put" {
			item, err := s.newItem(op.key, op.key, 0)
			if err != nil {
				t.Fatal(err)
			}
			s.put(item)
		}
		if op.op == "evict" {
			s.evict(op.key)
		}
		if op.op == "update" {
			item, _, err := s.update(
				op.key, func(value string) (string, time.Duration, error) {
					return value + value, 0, nil
				},
			)
			if err != nil {
				t.Fatal(err)
			}

			v, err := s.decode(item)
			if err != nil {
				t.Fatal(err)
			}

			got = append(got, v)
		}
	}

	if !reflect.DeepEqual(expected, got) {
		t.Fatalf("expected: %v\ngot:%v", expected, got)
	}
}
