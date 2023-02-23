package nitecache

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/pkg/errors"
)

var (
	addrReg                         = regexp.MustCompile("^[^:]+:[0-9]{2,5}$")
	ErrDuplicatePeer                = errors.New("duplicate peer detected")
	ErrInvalidPeerAddr              = errors.New("invalid peer address")
	ErrTableNotFound                = errors.New("table not found")
	ErrMissingSelfInPeers           = errors.New("peers must contain the current node")
	ErrMissingPeer                  = errors.New("peers must contain at least one member")
	ErrInitAlreadyCalled            = errors.New("Init already called")
	ErrMissingPeerDiscoveryInterval = errors.New("missing field PeerDiscoveryInterval in CacheOpts")
	unableToSetPeersErrStr          = "unable to set peers"
)

type Peer struct {
	ID   string
	Addr string
}

type CacheOpts struct {
	//Callback function used to initialize peers and detect changes. If PeerDiscovery is supplied, PeerDiscoveryInterval must also be supplied.
	PeerDiscovery         func() []Peer
	PeerDiscoveryInterval time.Duration
	//Defaults to 64
	VirtualNodes int
}

type itable interface {
	getLocally(key string) (item, error)
	putLocally(key string, value item)
	evictLocally(key string)
	executeLocally(key, function string, args []byte) (item, error)
	decode(b []byte) (item, error)
}

var isInit bool
var pself *Peer
var pclients clients
var ppicker peerPicker
var pmu *sync.RWMutex
var tables map[string]itable
var metrics *Metrics

// Creates a new nitecache instance
//
// Init should only be called once. Calling it twice will return an error
func Init(self Peer, opts CacheOpts) error {
	if isInit {
		return ErrInitAlreadyCalled
	}
	isInit = true

	pself = &self
	if ok := addrReg.MatchString(self.Addr); !ok {
		return fmt.Errorf("%w: %v", ErrInvalidPeerAddr, self.Addr)
	}

	tables = make(map[string]itable)
	pmu = &sync.RWMutex{}
	pclients = clients{}

	if opts.VirtualNodes == 0 {
		opts.VirtualNodes = 64
	}
	ppicker = newConsistentHasingPeerPicker(&self, opts.VirtualNodes)

	metrics = newMetrics()

	if opts.PeerDiscovery == nil {
		//Single node
		peers := []Peer{self}
		setPeers(peers)
	} else {
		//Multi node
		if opts.PeerDiscoveryInterval == 0 {
			return ErrMissingPeerDiscoveryInterval
		}

		go func() {
			//try to set peers right away
			peers := opts.PeerDiscovery()
			if err := setPeers(peers); err != nil {
				panic(fmt.Errorf("%v: %w", unableToSetPeersErrStr, err))
			}

			ticker := time.NewTicker(opts.PeerDiscoveryInterval)

			for range ticker.C {
				peers := opts.PeerDiscovery()
				if err := setPeers(peers); err != nil {
					panic(fmt.Errorf("%v: %w", unableToSetPeersErrStr, err))
				}
			}
		}()
	}

	go func() {
		if err := newServer(self.Addr); err != nil {
			panic(fmt.Errorf("unable to start cache server: %w", err))
		}
	}()

	return nil
}

func setPeers(peers []Peer) error {
	if len(peers) == 0 {
		return ErrMissingPeer
	}

	var containsSelf bool
	for i, p := range peers {
		if ok := addrReg.MatchString(p.Addr); !ok {
			return fmt.Errorf("%w: %v", ErrInvalidPeerAddr, p.Addr)
		}

		if pself.ID == p.ID {
			containsSelf = true
		}

		for n := i + 1; n < i; n++ {
			if peers[n].Addr == p.Addr {
				return fmt.Errorf("%w for Address %v", ErrDuplicatePeer, p.Addr)
			}
			if peers[n].ID == p.ID {
				return fmt.Errorf("%w for ID %v", ErrDuplicatePeer, p.ID)
			}
		}
	}
	if !containsSelf {
		return ErrMissingSelfInPeers
	}

	pmu.Lock()
	defer pmu.Unlock()

	ppicker.set(peers)
	if err := pclients.set(peers); err != nil {
		return err
	}

	return nil
}

func getOwner(key string) (*Peer, *client, error) {
	pmu.RLock()
	defer pmu.RUnlock()

	peer := ppicker.get(key)
	if peer == (&Peer{}) {
		return nil, nil, fmt.Errorf("unable to find peer for key %v", key)
	}

	client, ok := pclients[peer.ID]
	if !ok {
		return nil, nil, fmt.Errorf("unable to find peer client with ID %v", peer.ID)
	}

	return peer, client, nil
}

func getTable(name string) (itable, error) {
	t, ok := tables[name]
	if !ok {
		return nil, ErrTableNotFound
	}

	return t, nil
}

// Can safely be called from a goroutine, returns a copy of the current cache Metrics.
// For Metrics specific to a [Table], refer to [Table.GetMetrics]
func GetMetrics() Metrics {
	return metrics.getCopy()
}
