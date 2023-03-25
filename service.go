package nitecache

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/MysteriousPotato/nitecache/servicepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type service struct {
	servicepb.UnimplementedServiceServer
	cache *Cache
}

type client struct {
	conn *grpc.ClientConn
	servicepb.ServiceClient
	timeout time.Duration
}

type clients map[string]client

// We use a service mesh for automatic mTLS, but it would probably be better to support configuration for others who might not...
func newClient(addr string, timeout time.Duration) (client, error) {
	conn, err := grpc.Dial(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(timeoutInterceptor(timeout)),
	)
	if err != nil {
		return client{}, err
	}
	grpcClient := servicepb.NewServiceClient(conn)

	return client{
		ServiceClient: grpcClient,
		conn:          conn,
	}, nil
}

func newServer(addr string, cache *Cache) (*grpc.Server, func() error, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	servicepb.RegisterServiceServer(
		grpcServer, &service{
			cache: cache,
		},
	)

	start := func() error {
		return grpcServer.Serve(lis)
	}
	return grpcServer, start, nil
}

func timeoutInterceptor(timeout time.Duration) func(
	ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		ch := make(chan error)
		go func() {
			ch <- invoker(ctx, method, req, reply, cc, opts...)
		}()

		select {
		case err := <-ch:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s service) Get(_ context.Context, r *servicepb.GetRequest) (*servicepb.GetResponse, error) {
	t, err := s.cache.getTable(r.Table)
	if err != nil {
		return nil, err
	}

	item, err := t.getLocally(r.Key)
	if err != nil {
		return nil, err
	}

	return &servicepb.GetResponse{
		Item: &servicepb.Item{
			Expire: item.Expire.UnixMicro(),
			Value:  item.Value,
			Key:    item.Key,
		},
	}, nil
}

func (s service) Put(_ context.Context, r *servicepb.PutRequest) (*servicepb.EmptyResponse, error) {
	t, err := s.cache.getTable(r.Table)
	if err != nil {
		return nil, err
	}

	t.putLocally(
		item{
			Expire: time.UnixMicro(r.Item.Expire),
			Value:  r.Item.Value,
			Key:    r.Item.Key,
		},
	)
	return &servicepb.EmptyResponse{}, nil
}

func (s service) Evict(_ context.Context, r *servicepb.EvictRequest) (*servicepb.EmptyResponse, error) {
	t, err := s.cache.getTable(r.Table)
	if err != nil {
		return nil, err
	}

	t.evictLocally(r.Key)
	return &servicepb.EmptyResponse{}, nil
}

func (s service) Execute(_ context.Context, r *servicepb.ExecuteRequest) (*servicepb.ExecuteResponse, error) {
	t, err := s.cache.getTable(r.Table)
	if err != nil {
		return nil, err
	}

	item, err := t.executeLocally(r.Key, r.Function, r.Args)
	if err != nil {
		return nil, err
	}

	return &servicepb.ExecuteResponse{
		Item: &servicepb.Item{
			Expire: item.Expire.UnixMicro(),
			Value:  item.Value,
			Key:    item.Key,
		},
	}, nil
}

// Cleanup clients that are not present in peers and create new clients for new peers
func (cs clients) set(peers []Member, timeout time.Duration) error {
	if timeout == 0 {
		timeout = time.Second * 2
	}

	peersMap := map[string]Member{}
	for _, p := range peers {
		peersMap[p.ID] = p
	}

	var errs []error
	for id := range cs {
		if _, ok := peersMap[id]; !ok {
			if err := cs[id].conn.Close(); err != nil {
				errs = append(errs, err)
			}
			delete(cs, id)
		}
	}

	for id, p := range peersMap {
		if _, ok := cs[id]; ok {
			continue
		}
		client, err := newClient(p.Addr, timeout)
		if err != nil {
			return err
		}
		cs[id] = client
	}

	if errs != nil {
		return errors.Join(errs...)
	}
	return nil
}
