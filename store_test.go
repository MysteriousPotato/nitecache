package nitecache

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestStore(t *testing.T) {
	s := newStore(storeOpts[string]{
		getter: func(_ string) (string, time.Duration, error) {
			return "empty", 0, nil
		},
	})

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
	expected := []string{"empty", "1", "empty", "1", "1 1", "empty empty"}
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
			item, err := s.update(
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

func TestAutoCodecDetection(t *testing.T) {
	stringStore := newStore(storeOpts[string]{})

	str := "test"
	encodedStr, err := stringStore.codec.Encode(str)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(string(encodedStr), str) {
		t.Fatalf("expected %s, got %s", str, string(encodedStr))
	}

	bytesStore := newStore(storeOpts[[]byte]{})
	bytes := []byte("test")
	encodedBytes, err := bytesStore.codec.Encode(bytes)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(encodedBytes, bytes) {
		t.Fatalf("expected %s, got %s", string(bytes), string(encodedBytes))
	}

	mapsStore := newStore(storeOpts[map[string]string]{})
	expectedMap := []byte(`{"key":"value"}`)
	encodedMap, err := mapsStore.codec.Encode(map[string]string{"key": "value"})
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(encodedMap, expectedMap) {
		t.Fatalf("expected %s, got %s", string(expectedMap), string(encodedMap))
	}
}
