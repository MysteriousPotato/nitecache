package hashring

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type (
	HashFunc func(key string) (int, error)
	Opt      struct {
		Members      []string
		VirtualNodes int
		HashFunc     func(key string) (int, error)
	}
	Ring struct {
		hashFunc     func(key string) (int, error)
		hashMap      map[int]string
		points       []int
		mu           *sync.RWMutex
		members      []string
		virtualNodes int
	}
)

func New(opt Opt) (*Ring, error) {
	r := &Ring{
		hashFunc:     opt.HashFunc,
		hashMap:      map[int]string{},
		points:       []int{},
		mu:           &sync.RWMutex{},
		members:      opt.Members,
		virtualNodes: opt.VirtualNodes,
	}

	if err := r.populate(); err != nil {
		return nil, fmt.Errorf("unable to populate hasring: %w", err)
	}

	return r, nil
}

func DefaultHashFunc(key string) (int, error) {
	hash := fnv.New64()
	if _, err := hash.Write([]byte(key)); err != nil {
		return 0, fmt.Errorf("unable to populate Ring: %w", err)
	}
	return int(hash.Sum64()), nil
}

func (r *Ring) GetOwner(key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.members) == 1 {
		return r.members[0], nil
	}

	sum, err := r.hashFunc(key)
	if err != nil {
		return "", err
	}

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

func (r *Ring) SetMembers(newMembers []string) error {
	//We don't need points for a single node
	if len(newMembers) == 1 {
		r.members = newMembers
		r.clearPoints()
		return nil
	}

	if SliceEquals(newMembers, r.members) {
		return nil
	}

	//Create a new Ring, in order to minimize downtime
	ring := Ring{
		mu:           &sync.RWMutex{},
		points:       []int{},
		hashMap:      map[int]string{},
		members:      newMembers,
		hashFunc:     r.hashFunc,
		virtualNodes: r.virtualNodes,
	}

	if err := ring.populate(); err != nil {
		return fmt.Errorf("unable to populate hasring: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	*r = ring

	return nil
}

func (r *Ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	members := make([]string, len(r.members))
	copy(members, r.members)

	return members
}

func (r *Ring) Points() []int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	points := make([]int, len(r.points))
	copy(points, r.points)

	return points
}

func (r *Ring) VirtualNodes() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.virtualNodes
}

func (r *Ring) populate() error {
	r.clearPoints()

	//To make sure the collision prevention mechanism produces consistent results across all members, we need to sort members
	sort.Slice(
		r.members, func(i, j int) bool {
			return r.members[i] < r.members[j]
		},
	)

	for i, m := range r.members {
		for n := 0; n < r.virtualNodes; n++ {
			key := strconv.Itoa(n) + m

			//Avoid collisions by prefixing the hash until a unique point is created
			for prefix := ""; ; prefix += "-" {
				hash, err := r.hashFunc(prefix + key)
				if err != nil {
					return fmt.Errorf("unable to populate Ring: %w", err)
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

func (r *Ring) clearPoints() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.points = []int{}
	r.hashMap = map[int]string{}
}

// SliceEquals checks for order independent equality
func SliceEquals(slice1 []string, slice2 []string) bool {
	if len(slice1) != len(slice2) {
		return false
	}

	newMembersMap := make(map[string]struct{}, len(slice1))
	for _, newMember := range slice1 {
		newMembersMap[newMember] = struct{}{}
	}
	for _, member := range slice2 {
		if _, ok := newMembersMap[member]; !ok {
			return false
		}
		delete(newMembersMap, member)
	}

	return len(newMembersMap) == 0
}
