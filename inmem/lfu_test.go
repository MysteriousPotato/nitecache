package inmem_test

import (
	"github.com/MysteriousPotato/nitecache/inmem"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

func TestLfu(t *testing.T) {
	ops := []struct {
		key   int
		value int
	}{
		{key: 1, value: 0},
		{key: 1, value: 1},
		{key: 1, value: 2},
		{key: 2, value: 3},
		{key: 2, value: 4},
		{key: 4, value: 5},
		{key: 3, value: 6},
		{key: 2, value: 7},
		{key: 3, value: 8},
	}

	expected := map[int]int{1: 2, 2: 7, 3: 8}

	lfu := inmem.NewLFU[int, int](3)
	for _, op := range ops {
		lfu.Put(op.key, op.value)
	}

	for k, expectedV := range expected {
		v, ok := lfu.Get(k)
		if !ok {
			t.Fatalf("Value not found for key: %q", k)
		}
		if v != expectedV {
			t.Fatalf("Expected %v for key %q\ngot %v", expected, k, v)
		}
	}

	got := lfu.Values()
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected %v\ngot %v", expected, got)
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
