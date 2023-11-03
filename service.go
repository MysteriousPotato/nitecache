package nitecache

import (
	"context"
	"net"
	"time"

	"github.com/MysteriousPotato/nitecache/servicepb"
	"google.golang.org/grpc"
)

type service struct {
	servicepb.UnimplementedServiceServer
	cache *Cache
}

type server struct {
	listener net.Listener
	server   *grpc.Server
}

type (
	client struct {
		conn *grpc.ClientConn
		servicepb.ServiceClient
	}
	clients map[string]*client
)

func newClient(addr string, c *Cache) (*client, error) {
	conn, err := grpc.Dial(
		addr,
		grpc.WithTransportCredentials(c.transportCredentials),
		grpc.WithUnaryInterceptor(timeoutInterceptor(c.timeout)),
	)
	if err != nil {
		return nil, err
	}

	grpcClient := servicepb.NewServiceClient(conn)

	return &client{
		ServiceClient: grpcClient,
		conn:          conn,
	}, nil
}

func newService(addr string, cache *Cache) (server, error) {
	grpcServer := grpc.NewServer(cache.grpcOpts...)
	servicepb.RegisterServiceServer(grpcServer, &service{cache: cache})

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return server{}, err
	}

	return server{server: grpcServer, listener: listener}, nil
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

func (s service) Put(_ context.Context, r *servicepb.PutRequest) (*servicepb.Empty, error) {
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
	return &servicepb.Empty{}, nil
}

func (s service) Evict(_ context.Context, r *servicepb.EvictRequest) (*servicepb.Empty, error) {
	t, err := s.cache.getTable(r.Table)
	if err != nil {
		return nil, err
	}

	t.evictLocally(r.Key)
	return &servicepb.Empty{}, nil
}

func (s service) HealthCheck(_ context.Context, _ *servicepb.Empty) (*servicepb.Empty, error) {
	return &servicepb.Empty{}, nil
}

func (s service) Call(ctx context.Context, r *servicepb.CallRequest) (*servicepb.CallResponse, error) {
	t, err := s.cache.getTable(r.Table)
	if err != nil {
		return nil, err
	}

	item, err := t.callLocally(ctx, r.Key, r.Procedure, r.Args)
	if err != nil {
		return nil, err
	}

	return &servicepb.CallResponse{
		Item: &servicepb.Item{
			Expire: item.Expire.UnixMicro(),
			Value:  item.Value,
			Key:    item.Key,
		},
	}, nil
}
