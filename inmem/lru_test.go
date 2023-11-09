package inmem_test

import (
	"github.com/MysteriousPotato/nitecache/inmem"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

func TestLru(t *testing.T) {
	putOps := []struct {
		key    int
		value  int
		exists bool
	}{
		{key: 1, value: 0, exists: false},
		{key: 2, value: 1, exists: false},
		{key: 3, value: 2, exists: false},
		{key: 2, value: 3, exists: true},
		{key: 1, value: 4, exists: true},
		{key: 1, value: 5, exists: true},
		{key: 4, value: 6, exists: false},
		{key: 2, value: 7, exists: true},
		{key: 3, value: 8, exists: false},
	}
	expectedPut := map[int]int{2: 7, 3: 8, 4: 6}

	evictOps := []struct {
		key    int
		exists bool
	}{
		{key: 1, exists: false},
		{key: 2, exists: true},
		{key: 3, exists: true},
	}
	expectedEvict := map[int]int{4: 6}

	lru := inmem.NewLRU[int, int](3)
	for _, op := range putOps {
		if exists := lru.Put(op.key, op.value); exists != op.exists {
			t.Fatalf("Expected exists %t, got %t for put operation", op.exists, exists)
		}
	}

	for k, expectedV := range expectedPut {
		v, ok := lru.Get(k)
		if !ok {
			t.Fatalf("Value not found for key: %q", k)
		}
		if v != expectedV {
			t.Fatalf("Expected %v for key %q\ngot %v", expectedPut, k, v)
		}
	}

	got := lru.Values()
	if !reflect.DeepEqual(got, expectedPut) {
		t.Fatalf("Expected %v afcter puts \ngot %v", expectedPut, got)
	}

	for _, op := range evictOps {
		if exists := lru.Evict(op.key); exists != op.exists {
			t.Fatalf("Expected exists %t, got %t for evict operation", op.exists, exists)
		}
	}

	gotEvict := lru.Values()
	if !reflect.DeepEqual(gotEvict, expectedEvict) {
		t.Fatalf("Expected %v after evictions\ngot %v", expectedEvict, gotEvict)
	}
}

func TestLRUConcurrentAccess(t *testing.T) {
	goroutinesCount := 100
	iterations := 1000

	lru := inmem.NewLRU[int, int](128)
	wg := sync.WaitGroup{}

	wg.Add(goroutinesCount)
	for i := 0; i < goroutinesCount; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				lru.Put(j, j)
				lru.Get(j)
				lru.Inc(j)
				lru.Evict(j)
			}
		}()
	}

	wg.Wait()
}

func BenchmarkLRUPut(b *testing.B) {
	for _, threshold := range []int{10, 100, 1000, 10000, 100000} {
		b.Run("threshold="+strconv.Itoa(threshold), func(b *testing.B) {
			lru := inmem.NewLRU[int, int](threshold)
			for i := 0; i < b.N; i++ {
				lru.Put(i, i)
			}
		})
	}
}

func BenchmarkLRUGet(b *testing.B) {
	for _, threshold := range []int{10, 100, 1000, 10000, 100000} {
		b.Run("threshold="+strconv.Itoa(threshold), func(b *testing.B) {
			lru := inmem.NewLRU[int, int](threshold)
			for i := 0; i < b.N; i++ {
				lru.Put(i, i)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				lru.Get(i)
			}
		})
	}
}

func BenchmarkLRUEvict(b *testing.B) {
	for _, threshold := range []int{10, 100, 1000, 10000, 100000} {
		b.Run("threshold="+strconv.Itoa(threshold), func(b *testing.B) {
			lru := inmem.NewLRU[int, int](threshold)
			for i := 0; i < b.N; i++ {
				lru.Put(i, i)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				lru.Evict(i)
			}
		})
	}
}

func BenchmarkLRUInc(b *testing.B) {
	for _, threshold := range []int{10, 100, 1000, 10000, 100000} {
		b.Run("threshold="+strconv.Itoa(threshold), func(b *testing.B) {
			lru := inmem.NewLRU[int, int](threshold)
			for i := 0; i < b.N; i++ {
				lru.Put(i, i)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				lru.Inc(i)
			}
		})
	}
}
