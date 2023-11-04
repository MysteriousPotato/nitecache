package nitecache

import (
	"context"
	"errors"
	"fmt"
	"github.com/MysteriousPotato/nitecache/inmem"
	"github.com/MysteriousPotato/nitecache/servicepb"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
	"time"

	"github.com/MysteriousPotato/nitecache/hashring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	ErrDuplicatePeer  = errors.New("duplicate peer detected")
	ErrTableNotFound  = errors.New("table not found")
	ErrMissingMembers = errors.New("peers must contain at least one member")
	ErrCacheDestroyed = errors.New("can't use cache after tear down")
)

type (
	CacheOpt func(c *Cache)
	// Cache hold the nitecache instance. The zero value is not read for use.
	//
	// Refer to [NewCache] for creating an instance.
	Cache struct {
		ring                 *hashring.Ring
		self                 Member
		clients              clients
		clientMu             *sync.Mutex
		tables               map[string]table
		tablesMu             *sync.Mutex
		metrics              *metrics
		virtualNodes         int
		hashFunc             hashring.HashFunc
		timeout              time.Duration
		members              []Member
		grpcOpts             []grpc.ServerOption
		service              server
		transportCredentials credentials.TransportCredentials
	}
)

type Member struct {
	ID   string
	Addr string
}

type table interface {
	getLocally(key string) (inmem.Item[[]byte], bool, error)
	putLocally(key string, item inmem.Item[[]byte]) error
	evictLocally(key string) error
	callLocally(ctx context.Context, key, procedure string, args []byte) (inmem.Item[[]byte], error)
	tearDown()
}

// NewCache Creates a new [Cache] instance
//
// This should only be called once for a same set of peers, so that connections can be reused.
//
// Create a new [Table] using [NewTable] if you need to store different values.
//
// Members must have a unique ID and Addr.
//
// Ex.:
//
//	func() {
//			self := nitecache.Member{ID: "1", Addr: "localhost:8000"}
//			c, err := nitecache.NewCache(self, nitecache.Member{self})
//			...
//	}
func NewCache(self Member, peers []Member, opts ...CacheOpt) (*Cache, error) {
	c := &Cache{
		self:                 self,
		clients:              clients{},
		clientMu:             &sync.Mutex{},
		tables:               make(map[string]table),
		tablesMu:             &sync.Mutex{},
		metrics:              newMetrics(),
		virtualNodes:         32,
		hashFunc:             hashring.DefaultHashFunc,
		timeout:              time.Second * 3,
		members:              []Member{},
		transportCredentials: insecure.NewCredentials(),
	}

	for _, opt := range opts {
		opt(c)
	}

	var peersIncludeSelf bool
	for _, peer := range peers {
		if peer.ID == self.ID {
			peersIncludeSelf = true
			break
		}
	}
	if !peersIncludeSelf {
		peers = append(peers, self)
	}

	var err error
	c.service, err = newService(self.Addr, c)
	if err != nil {
		return nil, fmt.Errorf("unable to create cache service: %w", err)
	}

	if err := c.SetPeers(peers); err != nil {
		return nil, err
	}

	return c, nil
}

// VirtualNodeOpt sets the number of points/node on the hashring
// Defaults to 32
func VirtualNodeOpt(nodes int) func(c *Cache) {
	return func(c *Cache) {
		c.virtualNodes = nodes
	}
}

// TimeoutOpt sets the timeout for grpc clients
// Defaults to 3 seconds
func TimeoutOpt(timeout time.Duration) func(c *Cache) {
	return func(c *Cache) {
		c.timeout = timeout
	}
}

// HashFuncOpt sets the hash function used to determine hashring keys
// Defaults to FNV-1 algorithm
func HashFuncOpt(hashFunc hashring.HashFunc) func(c *Cache) {
	return func(c *Cache) {
		c.hashFunc = hashFunc
	}
}

// GRPCTransportCredentials sets the credentials for the gRPC server
func GRPCTransportCredentials(opts credentials.TransportCredentials) func(c *Cache) {
	return func(c *Cache) {
		c.transportCredentials = opts
	}
}

// GRPCServerOpts sets the options when creating the gRPC service.
func GRPCServerOpts(opts ...grpc.ServerOption) func(c *Cache) {
	return func(c *Cache) {
		c.grpcOpts = opts
	}
}

// GetMetrics Returns a copy of the current cache Metrics.
// For Metrics specific to a [Table], refer to [Table.GetMetrics].
func (c *Cache) GetMetrics() (Metrics, error) {
	if c.isZero() {
		return Metrics{}, ErrCacheDestroyed
	}
	return c.metrics.getCopy(), nil
}

// SetPeers will update the cache members to the new value.
func (c *Cache) SetPeers(peers []Member) error {
	if c.isZero() {
		return ErrCacheDestroyed
	}

	if len(peers) == 0 {
		return ErrMissingMembers
	}

	membersAddrMap := map[string]struct{}{}
	membersIDMap := map[string]struct{}{}
	var containsSelf bool
	for _, p := range peers {
		if _, ok := membersAddrMap[p.Addr]; ok {
			return fmt.Errorf("%w for Address %v", ErrDuplicatePeer, p.Addr)
		}
		if _, ok := membersIDMap[p.ID]; ok {
			return fmt.Errorf("%w for ID %v", ErrDuplicatePeer, p.ID)
		}
		if c.self.ID == p.ID {
			containsSelf = true
		}
		membersAddrMap[p.Addr] = struct{}{}
		membersIDMap[p.ID] = struct{}{}
	}
	if !containsSelf {
		peers = append(peers, c.self)
	}

	members := make([]string, len(peers))
	for i, p := range peers {
		members[i] = p.ID
	}

	var err error
	if c.ring == nil {
		c.ring, err = hashring.New(hashring.Opt{
			Members:      members,
			VirtualNodes: c.virtualNodes,
			HashFunc:     c.hashFunc,
		})
		if err != nil {
			return fmt.Errorf("unable to create hashring: %w", err)
		}
	} else {
		if err := c.ring.SetMembers(members); err != nil {
			return fmt.Errorf("unable to update hashring: %w", err)
		}
	}

	if err := c.setClients(peers); err != nil {
		return err
	}

	return nil
}

// TearDown properly tears down all [Table] from [Cache], closes all client connections and stops the grpc server.
//
// Once called, using it or any of its table references cause [ErrCacheDestroyed] to be returned.
func (c *Cache) TearDown() error {
	if c.isZero() {
		return ErrCacheDestroyed
	}

	var errs []error
	for _, client := range c.clients {
		if err := client.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	c.service.server.GracefulStop()

	for i := range c.tables {
		c.tables[i].tearDown()
	}
	*c = Cache{}

	return errors.Join(errs...)
}

// ListenAndServe starts the cache grpc server
func (c *Cache) ListenAndServe() error {
	if c.isZero() {
		return ErrCacheDestroyed
	}
	return c.service.server.Serve(c.service.listener)
}

// HealthCheckPeers checks the status of the cache's grpc clients
func (c *Cache) HealthCheckPeers(ctx context.Context) error {
	if c.isZero() {
		return ErrCacheDestroyed
	}

	var errs []error
	for _, client := range c.clients {
		if _, err := client.HealthCheck(ctx, &servicepb.Empty{}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (c *Cache) getTable(name string) (table, error) {
	t, ok := c.tables[name]
	if !ok {
		return nil, ErrTableNotFound
	}

	return t, nil
}

func (c *Cache) getClient(p string) (*client, error) {
	cl, ok := c.clients[p]
	if !ok {
		return nil, fmt.Errorf("unable to find peer client with ID %v", p)
	}
	return cl, nil
}

// Cleanup clients that are not present in peers and create new clients for new peers
func (c *Cache) setClients(peers []Member) error {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()

	peersMap := map[string]Member{}
	for _, p := range peers {
		peersMap[p.ID] = p
	}

	var errs []error
	for id := range c.clients {
		if _, ok := peersMap[id]; !ok {
			if err := c.clients[id].conn.Close(); err != nil {
				errs = append(errs, err)
			}
			delete(c.clients, id)
		}
	}

	for id, p := range peersMap {
		if _, ok := c.clients[id]; ok {
			continue
		}
		client, err := newClient(p.Addr, c)
		if err != nil {
			return err
		}
		c.clients[id] = client
	}

	if errs != nil {
		return errors.Join(errs...)
	}
	return nil
}

func (c *Cache) isZero() bool {
	return c == nil || c.tables == nil
}
