package inmem_test

import (
	"github.com/MysteriousPotato/nitecache/inmem"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

func TestLfu(t *testing.T) {
	putOps := []struct {
		key    int
		value  int
		exists bool
	}{
		{key: 1, value: 0, exists: false},
		{key: 1, value: 1, exists: true},
		{key: 1, value: 2, exists: true},
		{key: 2, value: 3, exists: false},
		{key: 2, value: 4, exists: true},
		{key: 4, value: 5, exists: false},
		{key: 3, value: 6, exists: false},
		{key: 2, value: 7, exists: true},
		{key: 3, value: 8, exists: true},
	}
	expectedPut := map[int]int{1: 2, 2: 7, 3: 8}

	evictOps := []struct {
		key    int
		exists bool
	}{
		{key: 1, exists: true},
		{key: 2, exists: true},
		{key: 4, exists: false},
	}
	expectedEvict := map[int]int{3: 8}

	lfu := inmem.NewLFU[int, int](3)
	for _, op := range putOps {
		if exists := lfu.Put(op.key, op.value); exists != op.exists {
			t.Fatalf("Expected exists %t, got %t for put operation", op.exists, exists)
		}
	}

	for k, expectedV := range expectedPut {
		v, ok := lfu.Get(k)
		if !ok {
			t.Fatalf("Value not found for key: %q", k)
		}
		if v != expectedV {
			t.Fatalf("Expected %v for key %q\ngot %v", expectedPut, k, v)
		}
	}

	got := lfu.Values()
	if !reflect.DeepEqual(got, expectedPut) {
		t.Fatalf("Expected %v afcter puts \ngot %v", expectedPut, got)
	}

	for _, op := range evictOps {
		if exists := lfu.Evict(op.key); exists != op.exists {
			t.Fatalf("Expected exists %t, got %t for evict operation", op.exists, exists)
		}
	}

	gotEvict := lfu.Values()
	if !reflect.DeepEqual(gotEvict, expectedEvict) {
		t.Fatalf("Expected %v after evictions\ngot %v", expectedEvict, gotEvict)
	}
}

func TestLFUConcurrentAccess(t *testing.T) {
	goroutinesCount := 100
	iterations := 1000

	lfu := inmem.NewLFU[int, int](128)
	wg := sync.WaitGroup{}

	wg.Add(goroutinesCount)
	for i := 0; i < goroutinesCount; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				lfu.Put(j, j)
				lfu.Get(j)
				lfu.Inc(j)
				lfu.Evict(j)
			}
		}()
	}

	wg.Wait()
}

func BenchmarkLFUPut(b *testing.B) {
	for _, threshold := range []int{10, 100, 1000, 10000, 100000} {
		b.Run("threshold="+strconv.Itoa(threshold), func(b *testing.B) {
			lfu := inmem.NewLFU[int, int](threshold)
			for i := 0; i < b.N; i++ {
				lfu.Put(i, i)
			}
		})
	}
}

func BenchmarkLFUGet(b *testing.B) {
	for _, threshold := range []int{10, 100, 1000, 10000, 100000} {
		b.Run("threshold="+strconv.Itoa(threshold), func(b *testing.B) {
			lfu := inmem.NewLFU[int, int](threshold)
			for i := 0; i < b.N; i++ {
				lfu.Put(i, i)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				lfu.Get(i)
			}
		})
	}
}

func BenchmarkLFUEvict(b *testing.B) {
	for _, threshold := range []int{10, 100, 1000, 10000, 100000} {
		b.Run("threshold="+strconv.Itoa(threshold), func(b *testing.B) {
			lfu := inmem.NewLFU[int, int](threshold)
			for i := 0; i < b.N; i++ {
				lfu.Put(i, i)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				lfu.Evict(i)
			}
		})
	}
}

func BenchmarkLFUInc(b *testing.B) {
	for _, threshold := range []int{10, 100, 1000, 10000, 100000} {
		b.Run("threshold="+strconv.Itoa(threshold), func(b *testing.B) {
			lfu := inmem.NewLFU[int, int](threshold)
			for i := 0; i < b.N; i++ {
				lfu.Put(i, i)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				lfu.Inc(i)
			}
		})
	}
}
