package nitecache

import (
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"
)

var (
	addrReg               = regexp.MustCompile("^[^:]+:[0-9]{2,5}$")
	ErrDuplicatePeer      = errors.New("duplicate peer detected")
	ErrInvalidPeerAddr    = errors.New("invalid peer address")
	ErrTableNotFound      = errors.New("table not found")
	ErrMissingSelfInPeers = errors.New("peers must contain the current node")
	ErrMissingMembers     = errors.New("peers must contain at least one member")
)

type CacheOpt func(c *Cache)

type Cache struct {
	ring         *hashring
	selfID       string
	clients      clients
	mu           *sync.RWMutex
	tables       map[string]itable
	metrics      *Metrics
	closeCh      chan bool
	virtualNodes int
	//Defaults to FNV-1
	hashFunc HashFunc
	//Defaults to 2 seconds
	timeout time.Duration
	//opt to skip server start
	testMode bool
}

type itable interface {
	getLocally(key string) (item, error)
	putLocally(itm item)
	evictLocally(key string)
	executeLocally(key, function string, args []byte) (item, error)
	TearDown()
}

// NewCache Creates a new [Cache] instance
// This should only be called once for a same set of peers, so that gRPC connections can be reused
// Create a new [Table] instead if you need to store different values
func NewCache(selfID string, peers []Member, opts ...CacheOpt) (*Cache, error) {
	c := &Cache{
		selfID:       selfID,
		tables:       make(map[string]itable),
		mu:           &sync.RWMutex{},
		clients:      clients{},
		metrics:      newMetrics(),
		closeCh:      make(chan bool),
		virtualNodes: 32,
		hashFunc:     defaultHashFunc,
	}

	for _, opt := range opts {
		opt(c)
	}

	var self Member
	for _, p := range peers {
		if p.ID == selfID {
			self = p
			break
		}
	}
	if self == (Member{}) {
		return nil, ErrMissingSelfInPeers
	}

	if !c.testMode {
		server, start, err := newServer(self.Addr, c)
		if err != nil {
			return nil, fmt.Errorf("unable to create cache server: %w", err)
		}
		go func() {
			if err := start(); err != nil {
				panic(fmt.Errorf("unable to create cache server: %w", err))
			}
		}()
		go func() {
			ticker := time.NewTicker(time.Second)
			for range ticker.C {
				select {
				case <-c.closeCh:
					server.Stop()
				}
			}
		}()
	}

	if err := c.SetPeers(peers); err != nil {
		return nil, err
	}

	return c, nil
}

// VirtualNodeOpt sets the number of points on the hashring per node
func VirtualNodeOpt(nodes int) func(c *Cache) {
	return func(c *Cache) {
		c.virtualNodes = nodes
	}
}

// TimeoutOpt sets the timeout for grpc client timeout
func TimeoutOpt(timeout time.Duration) func(c *Cache) {
	return func(c *Cache) {
		c.timeout = timeout
	}
}

// HashFuncOpt sets the hash function used to determine hashring keys
func HashFuncOpt(hashFunc HashFunc) func(c *Cache) {
	return func(c *Cache) {
		c.hashFunc = hashFunc
	}
}

func testModeOpt(c *Cache) {
	c.testMode = true
}

// GetMetrics Can safely be called from a goroutine, returns a copy of the current cache Metrics.
// For Metrics specific to a [Table], refer to [Table.GetMetrics]
func (c *Cache) GetMetrics() Metrics {
	return c.metrics.getCopy()
}

func (c *Cache) SetPeers(peers []Member) error {
	if len(peers) == 0 {
		return ErrMissingMembers
	}

	membersAddrMap := map[string]any{}
	membersIDMap := map[string]any{}
	var containsSelf bool
	for _, p := range peers {
		if ok := addrReg.MatchString(p.Addr); !ok {
			return fmt.Errorf("%w: %v", ErrInvalidPeerAddr, p.Addr)
		}
		if _, ok := membersAddrMap[p.Addr]; ok {
			return fmt.Errorf("%w for Address %v", ErrDuplicatePeer, p.Addr)
		}
		if _, ok := membersIDMap[p.ID]; ok {
			return fmt.Errorf("%w for ID %v", ErrDuplicatePeer, p.ID)
		}
		if c.selfID == p.ID {
			containsSelf = true
		}
		membersAddrMap[p.Addr] = nil
		membersIDMap[p.ID] = nil
	}
	if !containsSelf {
		return ErrMissingSelfInPeers
	}

	members := make(Members, len(peers))
	for i, p := range peers {
		members[i] = p
	}

	var err error
	if c.ring == nil {
		c.ring, err = newRing(
			ringCfg{
				Members:      members,
				VirtualNodes: c.virtualNodes,
				HashFunc:     c.hashFunc,
			},
		)
		if err != nil {
			return fmt.Errorf("unable to create hashring: %w", err)
		}
	} else {
		if err := c.ring.setMembers(members); err != nil {
			return fmt.Errorf("unable to update hashring: %w", err)
		}
	}

	if err := c.clients.set(peers, c.timeout); err != nil {
		return err
	}

	return nil
}

// TearDown Call this whenever a cache is not needed anymore.
//
// It will properly teardown all [Table]s from [Cache], close all client connections and stop the gRPC server
func (c *Cache) TearDown() error {
	var errs []error
	// Stop server on next tick
	c.closeCh <- true
	// Close all client connections
	for _, c := range c.clients {
		if err := c.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	// Teardown tables
	for _, t := range c.tables {
		t.TearDown()
	}

	if errs != nil {
		return errors.Join(errs...)
	}
	return nil
}

func (c *Cache) getTable(name string) (itable, error) {
	t, ok := c.tables[name]
	if !ok {
		return nil, ErrTableNotFound
	}

	return t, nil
}

func (c *Cache) getClient(p Member) (client, error) {
	cl, ok := c.clients[p.ID]
	if !ok {
		return client{}, fmt.Errorf("unable to find peer client with ID %v", p.ID)
	}
	return cl, nil
}
