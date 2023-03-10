package exemple

import (
	"context"
	"errors"
	"time"

	"github.com/MysteriousPotato/nitecache"
)

type Session struct {
	//All fields must be public since nitecache uses json/encoding to encode/decode values
	UserID   string
	Username string
}

func main() {
	//Both ID and Addr must be unque across peers
	self := nitecache.Peer{
		ID:   "1",
		Addr: "localhost:8100",
	}

	if err := nitecache.Init(self, nitecache.CacheOpts{
		VirtualNodes:          25,
		PeerDiscoveryInterval: time.Second * 5,
		PeerDiscovery: func() []nitecache.Peer {
			return getPeersFromSomewhere()
		},
	}); err != nil {
		panic(err)
	}

	//Creates a table called "sessions" containing Session values
	table := nitecache.NewTable[Session]("session").
		WithEvictionPolicy(nitecache.NewLruPolicy(256<<20)).
		WithGetter(func(key string) (Session, time.Duration, error) {
			//Cache-aside getter
			sess, err := getSessionFromSomewhere()
			if err != nil {
				return Session{}, time.Duration(0), err
			}

			return sess, time.Hour, nil
		}).
		WithFunction("updateUsername", func(s Session, args []byte) (Session, time.Duration, error) {
			s.Username = string(args)
			return s, 0, nil
		}).
		Build()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()

	sess, err := table.Get(ctx, "key")
	if errors.Is(err, nitecache.ErrTableKeyNotFound) {
		//handle key not found
	}
	if err != nil {
		panic(err)
	}

	if err := table.Put(ctx, "key", sess, time.Hour); err != nil {
		panic(err)
	}

	if err := table.Evict(ctx, "key"); err != nil {
		panic(err)
	}

	//Executes previously registered function "updateUsername" with a new username as args
	sess, err = table.Execute(ctx, "key", "updateUsername", []byte("new username"))
	if err != nil {
		panic(err)
	}
}

func getPeersFromSomewhere() []nitecache.Peer {
	return []nitecache.Peer{
		{ID: "1", Addr: "node1:8100"},
		{ID: "2", Addr: "node1:8200"},
	}
}

func getSessionFromSomewhere() (Session, error) {
	return Session{
		UserID:   "1",
		Username: "username",
	}, nil
}
