package inmem_test

import (
	"github.com/MysteriousPotato/nitecache/inmem"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

func TestLru(t *testing.T) {
	ops := []struct {
		key   int
		value int
	}{
		{key: 1, value: 0},
		{key: 2, value: 1},
		{key: 3, value: 2},
		{key: 2, value: 3},
		{key: 1, value: 4},
		{key: 1, value: 5},
		{key: 4, value: 6},
		{key: 2, value: 7},
		{key: 3, value: 8},
	}
	expected := map[int]int{2: 7, 3: 8, 4: 6}

	lru := inmem.NewLRU[int, int](3)
	for _, op := range ops {
		lru.Put(op.key, op.value)
	}

	for k, expectedV := range expected {
		v, ok := lru.Get(k)
		if !ok {
			t.Fatalf("Value not found for key: %q", k)
		}
		if v != expectedV {
			t.Fatalf("Expected %v for key %q\ngot %v", expected, k, v)
		}
	}

	got := lru.Values()
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected %v\ngot %v", expected, got)
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
