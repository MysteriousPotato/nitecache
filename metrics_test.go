package nitecache_test

import (
	"context"
	"errors"
	"github.com/MysteriousPotato/nitecache"
	"reflect"
	"testing"
	"time"

	test "github.com/MysteriousPotato/nitecache/test_utils"
)

func TestMetrics(t *testing.T) {
	ctx := context.Background()
	c, err := nitecache.NewCache(
		nitecache.Member{ID: "1", Addr: test.GetUniqueAddr()},
		[]nitecache.Member{
			{ID: "1", Addr: test.GetUniqueAddr()},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	tables := []*nitecache.Table[int]{
		nitecache.NewTable[int]("table-1").
			WithProcedure("function", func(_ context.Context, v int, args []byte) (int, time.Duration, error) {
				return v, 0, nil
			}).
			Build(c),
		nitecache.NewTable[int]("table-2").
			WithProcedure("function", func(_ context.Context, v int, args []byte) (int, time.Duration, error) {
				return v, 0, nil
			}).
			Build(c),
	}

	ops := []struct {
		op  string
		val string
		key string
	}{
		{op: "get", key: "1"},
		{op: "put", key: "1"},
		{op: "get", key: "1"},
		{op: "update", key: "1"},
		{op: "evict", key: "1"},
		{op: "update", key: "2"},
	}

	for _, table := range tables {
		for _, op := range ops {
			if op.op == "get" {
				if _, err := table.Get(ctx, op.key); err != nil && !errors.Is(err, nitecache.ErrKeyNotFound) {
					t.Fatal(err)
				}
			}
			if op.op == "put" {
				if err := table.Put(ctx, op.key, 0, 0); err != nil {
					t.Fatal(err)
				}
			}
			if op.op == "evict" {
				if err := table.Evict(ctx, op.key); err != nil {
					t.Fatal(err)
				}
			}
			if op.op == "update" {
				if _, err := table.Call(ctx, op.key, "function", []byte{}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	expectedGlobal := nitecache.Metrics{
		Miss:  2,
		Get:   4,
		Put:   2,
		Evict: 2,
		Call: map[string]int64{
			"function": 4,
		},
	}
	expectedTable := nitecache.Metrics{
		Miss:  1,
		Get:   2,
		Put:   1,
		Evict: 1,
		Call: map[string]int64{
			"function": 2,
		},
	}

	gotTable1, err := tables[0].GetMetrics()
	if err != nil {
		t.Fatal(err)
	}
	gotTable2, err := tables[1].GetMetrics()
	if err != nil {
		t.Fatal(err)
	}
	gotGlobal, err := c.GetMetrics()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expectedGlobal, gotGlobal) {
		t.Fatalf("expected global metrics: %+v\ngot:%+v", expectedGlobal, gotGlobal)
	}

	if !reflect.DeepEqual(expectedTable, gotTable1) {
		t.Fatalf("expected table metrics: %+v\ngot:%+v", expectedTable, gotTable1)
	}

	if !reflect.DeepEqual(expectedTable, gotTable2) {
		t.Fatalf("expected table metrics: %+v\ngot:%+v", expectedTable, gotTable2)
	}

	if err = c.TearDown(); err != nil {
		t.Fatal(err)
	}

	if _, err := tables[0].GetMetrics(); !errors.Is(err, nitecache.ErrCacheDestroyed) {
		t.Fatalf("expected err: %v\ngot:%v", nitecache.ErrCacheDestroyed, err)
	}
	if _, err := c.GetMetrics(); !errors.Is(err, nitecache.ErrCacheDestroyed) {
		t.Fatalf("expected err: %v\ngot:%v", nitecache.ErrCacheDestroyed, err)
	}
}
