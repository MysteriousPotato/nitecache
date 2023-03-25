package nitecache

import (
	"crypto/md5"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type HashFunc func(key string) (int, error)

type ringCfg struct {
	Members      []Member
	VirtualNodes int
	HashFunc     func(key string) (int, error)
}

type hashring struct {
	hashFunc     func(key string) (int, error)
	hashMap      map[int]Member
	points       []int
	mu           *sync.RWMutex
	members      Members
	virtualNodes int
}

func newRing(cfg ringCfg) (*hashring, error) {
	r := &hashring{
		hashFunc:     cfg.HashFunc,
		hashMap:      map[int]Member{},
		points:       []int{},
		mu:           &sync.RWMutex{},
		members:      cfg.Members,
		virtualNodes: cfg.VirtualNodes,
	}
	if r.hashFunc == nil {
		r.hashFunc = defaultHashFunc
	}
	if r.virtualNodes <= 0 {
		r.virtualNodes = 32
	}

	if err := r.populate(); err != nil {
		return nil, fmt.Errorf("unable to populate hasring: %w", err)
	}

	return r, nil
}

func defaultHashFunc(key string) (int, error) {
	hash := fnv.New64()
	if _, err := hash.Write([]byte(key)); err != nil {
		return 0, fmt.Errorf("unable to populate hashring: %w", err)
	}
	return int(hash.Sum64()), nil
}

func (r *hashring) getOwner(key string) (Member, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.members) == 1 {
		return r.members[0], nil
	}

	hash := fnv.New64()
	if _, err := hash.Write([]byte(key)); err != nil {
		return Member{}, fmt.Errorf("unable to get write hash: %w", err)
	}
	sum := int(hash.Sum64())

	i := sort.Search(
		len(r.points), func(i int) bool {
			return r.points[i] >= sum
		},
	)
	if i == len(r.points) {
		i = 0
	}

	return r.hashMap[r.points[i]], nil
}

func (r *hashring) setMembers(newMembers Members) error {
	//We don't need points for a single node
	if len(newMembers) == 1 {
		r.members = newMembers
		r.clearPoints()
		return nil
	}

	//In case no changes happened
	if r.members.equals(newMembers) {
		return nil
	}

	//Create a new hashring, in order to minimize downtime
	newRing := hashring{
		mu:           &sync.RWMutex{},
		points:       []int{},
		hashMap:      map[int]Member{},
		members:      newMembers,
		hashFunc:     r.hashFunc,
		virtualNodes: r.virtualNodes,
	}

	if err := newRing.populate(); err != nil {
		return fmt.Errorf("unable to populate hasring: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	*r = newRing

	return nil
}

func (r *hashring) populate() error {
	r.clearPoints()

	//To make sure the collision prevention mechanism produces consistent results for identical Members, we need to sort Members
	sort.Slice(
		r.members, func(i, j int) bool {
			return r.members[i].ID < r.members[j].ID
		},
	)

	for i, m := range r.members {
		for n := 0; n < r.virtualNodes; n++ {
			keyHash := md5.Sum([]byte(strconv.Itoa(n) + m.ID))

			//Avoid collisions by prefixing the hash until a unique point is created
			for prefix := ""; ; prefix += "-" {
				hash, err := r.hashFunc(prefix + string(keyHash[:]))
				if err != nil {
					return fmt.Errorf("unable to populate hashring: %w", err)
				}

				if _, ok := r.hashMap[hash]; ok {
					continue
				}

				r.hashMap[hash] = r.members[i]
				r.points = append(r.points, hash)
				break
			}
		}
	}
	sort.Ints(r.points)
	return nil
}

func (r *hashring) clearPoints() {
	r.points = []int{}
	r.hashMap = map[int]Member{}
}
