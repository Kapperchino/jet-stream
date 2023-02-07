package client

import (
	"github.com/Kapperchino/jet-application/proto/proto"
	clusterPb "github.com/Kapperchino/jet-cluster/proto/proto"
	"github.com/buraksezer/consistent"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	"time"
)

func newHashRing() *consistent.Consistent {
	cfg := consistent.Config{
		PartitionCount:    0,
		ReplicationFactor: 0,
		Load:              0,
		Hasher:            hasher{},
	}
	return consistent.New(nil, cfg)
}

func newClusterClient(conn *grpc.ClientConn) clusterPb.ClusterMetaServiceClient {
	client := clusterPb.NewClusterMetaServiceClient(conn)
	return client
}

func newMessageClient(conn *grpc.ClientConn) proto.MessageServiceClient {
	client := proto.NewMessageServiceClient(conn)
	return client
}

func newClientConnection(address string) (*grpc.ClientConn, error) {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "Example"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	maxSize := 1 * 1024 * 1024 * 1024
	return grpc.Dial(address,
		grpc.WithDefaultServiceConfig(serviceConfig), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(false),
			grpc.MaxCallRecvMsgSize(maxSize),
			grpc.MaxCallSendMsgSize(maxSize)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
}
