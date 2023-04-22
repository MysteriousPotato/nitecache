package nitecache

import (
	"context"
	"errors"
	"github.com/MysteriousPotato/nitecache/test"
	"reflect"
	"testing"
	"time"
)

func TestCache_SetPeers(t *testing.T) {
	c, err := NewCache(
		"potato",
		[]Member{{ID: "potato", Addr: test.GetUniqueAddr()}},
		testModeOpt,
	)
	if err != nil {
		t.Error(err)
	}

	tests := []struct {
		name     string
		members  []Member
		expected error
	}{
		{
			name:     "invalid address",
			expected: ErrInvalidPeerAddr,
			members: Members{
				{
					ID:   "potato",
					Addr: "potato",
				},
			},
		}, {
			name:     "members not including current node",
			expected: ErrMissingSelfInPeers,
			members: Members{
				{
					ID:   "zucchini",
					Addr: test.GetUniqueAddr(),
				},
			},
		}, {
			name:     "duplicate member",
			expected: ErrDuplicatePeer,
			members: Members{
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
			expected: ErrMissingMembers,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				if err := c.SetPeers(tt.members); !errors.Is(err, tt.expected) {
					t.Errorf("expected err: %v, got: %v", tt.expected, err)
				}
			},
		)
	}
}

func TestSingleNodeCacheTable(t *testing.T) {
	self := Member{
		ID:   "1",
		Addr: test.GetUniqueAddr(),
	}

	c, err := NewCache(self.ID, []Member{self})
	if err != nil {
		t.Error(err)
	}
	defer test.TearDown(c)

	table := NewTable[string]("tt").
		WithGetter(
			func(key string) (string, time.Duration, error) {
				return "empty", time.Hour, nil
			},
		).
		WithFunction(
			"execute", func(s string, args []byte) (string, time.Duration, error) {
				return "execute", 0, nil
			},
		).
		Build(c)

	ctx := context.Background()
	tests := []struct {
		op    string
		key   string
		value string
	}{
		{op: "get", key: "1"},
		{op: "put", key: "1", value: "1"},
		{op: "get", key: "1"},
		{op: "execute", key: "1"},
		{op: "evict", key: "1"},
		{op: "get", key: "1"},
		{op: "put", key: "1", value: "2"},
	}
	expected := []string{"empty", "1", "execute", "empty"}
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
		case "execute":
			v, err := table.Execute(ctx, "key", "execute", []byte{})
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
}

func TestMultiNodeCacheTable(t *testing.T) {
	members := []Member{
		{
			ID:   "1",
			Addr: test.GetUniqueAddr(),
		}, {
			ID:   "2",
			Addr: test.GetUniqueAddr(),
		},
	}

	caches := make([]*Cache, len(members))
	tables := make([]*Table[string], len(members))
	for i, m := range members {
		func() {
			c, err := NewCache(m.ID, members)
			if err != nil {
				t.Error(err)
			}
			defer test.TearDown(c)

			caches[i] = c
			tables[i] = NewTable[string]("test").
				WithGetter(
					func(key string) (string, time.Duration, error) {
						return "empty", time.Hour, nil
					},
				).
				WithFunction(
					"execute", func(s string, args []byte) (string, time.Duration, error) {
						return "execute", 0, nil
					},
				).
				Build(c)
		}()
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
		{op: "execute", key: "1"},
		{op: "evict", key: "1"},
		{op: "get", key: "1"},
		{op: "put", key: "1", value: "2"},
		{op: "get", key: "3swerwedf"},
		{op: "put", key: "3swerwedf", value: "1"},
		{op: "get", key: "3swerwedf"},
		{op: "execute", key: "3swerwedf"},
		{op: "evict", key: "3swerwedf"},
		{op: "get", key: "3swerwedf"},
		{op: "put", key: "3swerwedf", value: "2"},
		{op: "evict", key: "1"},
		{op: "evict", key: "3swerwedf"},
	}

	for _, table := range tables {
		expected := []string{"empty", "1", "execute", "empty", "empty", "1", "execute", "empty"}
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
			case "execute":
				v, err := table.Execute(ctx, tt.key, "execute", []byte{})
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
