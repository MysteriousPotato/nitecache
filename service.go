package nitecache

import (
	"context"
	"net"

	"github.com/MysteriousPotato/nitecache/servicepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type service struct {
	servicepb.UnimplementedServiceServer
}

type client struct {
	conn *grpc.ClientConn
	servicepb.ServiceClient
}

type clients map[string]*client

// We use a service mesh for automatic mTLS, but it would probably be better to support configuration for others who might not...
func newClient(addr string) (*client, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := servicepb.NewServiceClient(conn)

	return &client{
		ServiceClient: c,
		conn:          conn,
	}, nil
}

func newServer(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	servicepb.RegisterServiceServer(grpcServer, &service{})

	return grpcServer.Serve(lis)
}

func (s service) Get(ctx context.Context, r *servicepb.GetRequest) (*servicepb.GetResponse, error) {
	t, err := getTable(r.Table)
	if err != nil {
		return &servicepb.GetResponse{}, err
	}

	item, err := t.getLocally(r.Key)
	if err != nil {
		return &servicepb.GetResponse{}, err
	}

	return &servicepb.GetResponse{
		Item: &servicepb.Item{
			Expire: item.Expire.UnixMicro(),
			Value:  item.Value,
			Key:    item.Key,
		},
	}, nil
}

func (s service) Put(ctx context.Context, r *servicepb.PutRequest) (*servicepb.EmptyResponse, error) {
	t, err := getTable(r.Table)
	if err != nil {
		return &servicepb.EmptyResponse{}, err
	}

	item, err := t.decode(r.Value.Value)
	if err != nil {
		return &servicepb.EmptyResponse{}, err
	}

	t.putLocally(r.Value.Key, item)

	return &servicepb.EmptyResponse{}, nil
}

func (s service) Evict(ctx context.Context, r *servicepb.EvictRequest) (*servicepb.EmptyResponse, error) {
	t, err := getTable(r.Table)
	if err != nil {
		return nil, err
	}

	t.evictLocally(r.Key)
	return &servicepb.EmptyResponse{}, nil
}

func (s service) Execute(ctx context.Context, r *servicepb.ExecuteRequest) (*servicepb.ExecuteResponse, error) {
	t, err := getTable(r.Table)
	if err != nil {
		return nil, err
	}

	item, err := t.executeLocally(r.Key, r.Function, r.Args)
	if err != nil {
		return nil, err
	}

	return &servicepb.ExecuteResponse{
		Value: &servicepb.Item{
			Expire: item.Expire.UnixMicro(),
			Value:  item.Value,
			Key:    item.Key,
		},
	}, nil
}

// Cleanup clients that are not present in peers and create new clients for new peers
func (cs clients) set(peers []Peer) error {
	peersMap := map[string]Peer{}
	for _, p := range peers {
		peersMap[p.ID] = p
	}

	for id := range cs {
		if _, ok := peersMap[id]; !ok {
			cs[id].conn.Close()
			delete(cs, id)
		}
	}

	for id, p := range peersMap {
		if _, ok := cs[id]; ok {
			continue
		}
		client, err := newClient(p.Addr)
		if err != nil {
			return err
		}
		cs[id] = client
	}
	return nil
}
