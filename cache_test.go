package nitecache_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/MysteriousPotato/nitecache"
	test "github.com/MysteriousPotato/nitecache/test_utils"
)

func TestCache_SetPeers(t *testing.T) {
	self := nitecache.Member{ID: "potato", Addr: test.GetUniqueAddr()}
	c, err := nitecache.NewCache(self, []nitecache.Member{self})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		members  []nitecache.Member
		expected error
	}{{
		name:     "duplicate member",
		expected: nitecache.ErrDuplicatePeer,
		members: []nitecache.Member{
			{
				ID:   "potato",
				Addr: test.GetUniqueAddr(),
			}, {
				ID:   "potato",
				Addr: test.GetUniqueAddr(),
			},
		},
	}, {
		name:     "no member",
		expected: nitecache.ErrMissingMembers,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.SetPeers(tt.members); !errors.Is(err, tt.expected) {
				t.Errorf("expected err: %v, got: %v", tt.expected, err)
			}
		})
	}

	if err := c.TearDown(); err != nil {
		t.Fatal(err)
	}

	if err := c.SetPeers(nil); !errors.Is(err, nitecache.ErrCacheDestroyed) {
		t.Fatalf("expected err: %v\ngot:%v", nitecache.ErrCacheDestroyed, err)
	}
}

func TestSingleNodeCacheTable(t *testing.T) {
	ctx := context.Background()
	self := nitecache.Member{
		ID:   "1",
		Addr: test.GetUniqueAddr(),
	}

	c, err := nitecache.NewCache(self, []nitecache.Member{self})
	if err != nil {
		t.Fatal(err)
	}

	table := nitecache.NewTable[string]("tt").
		WithGetter(
			func(key string) (string, time.Duration, error) {
				return "empty", time.Hour, nil
			},
		).
		WithProcedure("procedure", func(_ context.Context, _ string, _ []byte) (string, time.Duration, error) {
			return "procedure", 0, nil
		}).
		Build(c)

	tests := []struct {
		op    string
		key   string
		value string
	}{
		{op: "get", key: "1"},
		{op: "put", key: "1", value: "1"},
		{op: "get", key: "1"},
		{op: "call", key: "1"},
		{op: "evict", key: "1"},
		{op: "get", key: "1"},
		{op: "put", key: "1", value: "2"},
	}
	expected := []string{"empty", "1", "procedure", "empty"}
	var got []string

	for _, tt := range tests {
		switch tt.op {
		case "get":
			v, err := table.Get(ctx, "key")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, v)
			break
		case "put":
			if err := table.Put(ctx, "key", tt.value, time.Hour); err != nil {
				t.Fatal(err)
			}
			break
		case "evict":
			if err := table.Evict(ctx, "key"); err != nil {
				t.Fatal(err)
			}
			break
		case "call":
			v, err := table.Call(ctx, "key", "procedure", nil)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, v)
			break
		}
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("\nexpect: %v\ngot: %v", expected, got)
	}

	if err := c.TearDown(); err != nil {
		t.Fatal(err)
	}

	if _, err := table.Get(ctx, ""); !errors.Is(err, nitecache.ErrCacheDestroyed) {
		t.Fatalf("expected err: %v\ngot:%v", nitecache.ErrCacheDestroyed, err)
	}

	if err := table.Put(ctx, "", "", 0); !errors.Is(err, nitecache.ErrCacheDestroyed) {
		t.Fatalf("expected err: %v\ngot:%v", nitecache.ErrCacheDestroyed, err)
	}

	if err := table.Evict(ctx, ""); !errors.Is(err, nitecache.ErrCacheDestroyed) {
		t.Fatalf("expected err: %v\ngot:%v", nitecache.ErrCacheDestroyed, err)
	}

	if _, err := table.Call(ctx, "", "", nil); !errors.Is(err, nitecache.ErrCacheDestroyed) {
		t.Fatalf("expected err: %v\ngot:%v", nitecache.ErrCacheDestroyed, err)
	}
}

func TestMultiNodeCacheTable(t *testing.T) {
	members := []nitecache.Member{
		{
			ID:   "1",
			Addr: test.GetUniqueAddr(),
		}, {
			ID:   "2",
			Addr: test.GetUniqueAddr(),
		}, {
			ID:   "3",
			Addr: test.GetUniqueAddr(),
		},
	}

	caches := make([]*nitecache.Cache, len(members))
	tables := make([]*nitecache.Table[string], len(members))
	for i, m := range members {
		func() {
			c, err := nitecache.NewCache(
				m,
				members,
				nitecache.VirtualNodeOpt(1),
				nitecache.HashFuncOpt(test.SimpleHashFunc),
				nitecache.TimeoutOpt(time.Second*5),
			)
			if err != nil {
				t.Fatal(err)
			}

			go func() {
				if err := c.ListenAndServe(); err != nil {
					t.Error(err)
					return
				}
			}()

			caches[i] = c
			tables[i] = nitecache.NewTable[string]("test").
				WithGetter(
					func(key string) (string, time.Duration, error) {
						return "empty", time.Hour, nil
					},
				).
				WithProcedure(
					"procedure", func(_ context.Context, _ string, _ []byte) (string, time.Duration, error) {
						return "procedure", 0, nil
					},
				).
				Build(c)
		}()
	}

	for _, c := range caches {
		test.WaitForServer(t, c)
	}

	ctx := context.Background()
	tests := []struct {
		op    string
		key   string
		value string
	}{
		{op: "get", key: "1"},
		{op: "put", key: "1", value: "1"},
		{op: "get", key: "1"},
		{op: "call", key: "1"},
		{op: "evict", key: "1"},
		{op: "get", key: "1"},
		{op: "put", key: "1", value: "2"},
		{op: "get", key: "2"},
		{op: "put", key: "2", value: "1"},
		{op: "get", key: "2"},
		{op: "call", key: "2"},
		{op: "evict", key: "2"},
		{op: "get", key: "2"},
		{op: "put", key: "2", value: "2"},
		{op: "evict", key: "1"},
		{op: "evict", key: "2"},
	}

	for _, table := range tables {
		expected := []string{"empty", "1", "procedure", "empty", "empty", "1", "procedure", "empty"}
		var got []string

		for _, tt := range tests {
			switch tt.op {
			case "get":
				v, err := table.Get(ctx, tt.key)
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, v)
				break
			case "put":
				if err := table.Put(ctx, tt.key, tt.value, time.Hour); err != nil {
					t.Fatal(err)
				}
				break
			case "evict":
				if err := table.Evict(ctx, tt.key); err != nil {
					t.Fatal(err)
				}
				break
			case "call":
				v, err := table.Call(ctx, tt.key, "procedure", []byte{})
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, v)
				break
			}
		}
		if !reflect.DeepEqual(got, expected) {
			t.Errorf("expect: %v, got: %v", expected, got)
		}
	}
}
