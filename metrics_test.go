package nitecache

import (
	"context"
	"errors"
	"github.com/MysteriousPotato/nitecache/test"
	"reflect"
	"testing"
	"time"
)

func TestMetrics(t *testing.T) {
	c, err := NewCache("1", []Member{{ID: "1", Addr: test.GetUniqueAddr()}}, CacheOpts{})
	if err != nil {
		t.Fatal(err)
	}
	defer test.TearDown(c)

	tables := []*Table[int]{
		NewTable[int]("table-1").
			WithFunction(
				"function", func(v int, args []byte) (int, time.Duration, error) {
					return v, 0, nil
				},
			).
			Build(c),
		NewTable[int]("table-2").
			WithFunction(
				"function", func(v int, args []byte) (int, time.Duration, error) {
					return v, 0, nil
				},
			).
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
	ctx := context.TODO()

	for _, table := range tables {
		for _, op := range ops {
			if op.op == "get" {
				if _, err := table.Get(ctx, op.key); err != nil && !errors.Is(err, ErrKeyNotFound) {
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
				if _, err := table.Execute(ctx, op.key, "function", []byte{}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	expectedGlobal := Metrics{
		Miss:  2,
		Get:   4,
		Put:   2,
		Evict: 2,
		Execute: map[string]int64{
			"function": 4,
		},
	}
	expectedTable := Metrics{
		Miss:  1,
		Get:   2,
		Put:   1,
		Evict: 1,
		Execute: map[string]int64{
			"function": 2,
		},
	}

	gotTable1 := tables[0].GetMetrics()
	gotTable2 := tables[1].GetMetrics()
	gotGlobal := c.GetMetrics()

	if !reflect.DeepEqual(expectedGlobal, gotGlobal) {
		t.Fatalf("expected global: %+v\ngot:%+v", expectedGlobal, gotGlobal)
	}

	if !reflect.DeepEqual(expectedTable, gotTable1) {
		t.Fatalf("expected table: %+v\ngot:%+v", expectedTable, gotTable1)
	}

	if !reflect.DeepEqual(expectedTable, gotTable2) {
		t.Fatalf("expected table: %+v\ngot:%+v", expectedTable, gotTable2)
	}
}
