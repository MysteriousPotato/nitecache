package nitecache

import (
	"crypto/md5"
	"hash/crc32"
	"hash/fnv"
	"math"
	"strconv"
	"strings"
	"testing"
)

// This is not a test, but just a way to visualize a distribution example for the current consistent hash implementation
func TestConsistentHashDistribution(t *testing.T) {
	seeds := []string{"node-1", "node-2", "node-3"}
	vNodes := 64
	distribution := []uint64{}

	for _, s := range seeds {
		for i := 0; i < vNodes; i++ {
			hash := fnv.New64()
			keyHash := md5.Sum([]byte(strconv.Itoa(i) + s))
			hash.Write(keyHash[:])
			distribution = append(distribution, hash.Sum64())
		}
	}

	sectionsCount := uint64(20)
	sections := make([]int, sectionsCount)
	for i := uint64(0); i < sectionsCount; i++ {
		sectionMax := math.MaxUint64 / sectionsCount * i
		sectionMin := math.MaxUint64 / sectionsCount * (i - 1)

		for _, d := range distribution {
			if d < uint64(sectionMax) && d > uint64(sectionMin) {
				sections[i]++
			}
		}
	}

	for _, s := range sections {
		t.Log(strings.Repeat("|", s))
	}
}

func BenchmarkHash(b *testing.B) {
	seeds := []string{"potato", "carot", "zucchini"}

	b.Run("fnv1-32", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for n, s := range seeds {
				hash := fnv.New32()
				keyHash := md5.Sum([]byte(strconv.Itoa(n) + s))
				hash.Write(keyHash[:])
				hash.Sum32()
			}
		}
	})

	b.Run("fnv1-64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for n, s := range seeds {
				hash := fnv.New64()
				keyHash := md5.Sum([]byte(strconv.Itoa(n) + s))
				hash.Write(keyHash[:])
				hash.Sum64()
			}
		}
	})

	b.Run("crc-32", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for n, s := range seeds {
				keyHash := md5.Sum([]byte(strconv.Itoa(n) + s))
				crc32.ChecksumIEEE(keyHash[:])
			}
		}
	})
}
