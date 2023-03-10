package nitecache

import (
	"reflect"
	"testing"
)

func TestLru(t *testing.T) {
	ops := []struct {
		key string
	}{
		{key: "1"},
		{key: "2"},
		{key: "3"},
		{key: "2"},
		{key: "1"},
		{key: "1"},
		{key: "2"},
		{key: "3"},
	}

	expected := []string{"1", "3", "1"}
	got := []string{}

	lru := NewLruPolicy(2) //2bytes
	lru.onEvict = func(key string) {
		got = append(got, key)
	}

	for _, op := range ops {
		lru.push(op.key, item{Value: []byte{0}})
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected %v\ngot %v", expected, got)
	}
}

func TestLfu(t *testing.T) {
	ops := []struct {
		key string
	}{
		{key: "1"},
		{key: "1"},
		{key: "1"},
		{key: "2"},
		{key: "2"},
		{key: "3"},
		{key: "2"},
		{key: "3"},
	}

	expected := []string{"3", "3"}
	got := []string{}

	lfu := NewLfuPolicy(2) //2bytes
	lfu.onEvict = func(key string) {
		got = append(got, key)
	}

	for _, op := range ops {
		lfu.push(op.key, item{Value: []byte{0}})
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected %v\ngot %v", expected, got)
	}
}
