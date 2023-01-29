package client

import (
	"context"
	"github.com/Kapperchino/jet-application/proto"
	clusterPb "github.com/Kapperchino/jet-cluster/proto"
	"github.com/alphadose/haxmap"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"time"
)

type JetClient struct {
	info         *clusterPb.ClusterInfo
	shardClients *haxmap.Map[string, *ShardClient]
}

type ShardClient struct {
	leader        string
	memberclients []*MemberClient
}

type MemberClient struct {
	nodeId        string
	clusterClient clusterPb.ClusterMetaServiceClient
	messageClient proto.MessageServiceClient
}

func New(address string) (*JetClient, error) {
	con, err := newClientConnection(address)
	if err != nil {
		log.Error().Msgf("Error creating connection with client")
		return nil, err
	}
	ctx := context.Background()
	rootClient := newClusterClient(con)
	defer con.Close()
	clusterInfo, err := rootClient.GetClusterInfo(ctx, &clusterPb.GetClusterInfoRequest{})
	if err != nil {
		log.Err(err).Msgf("Error getting cluster info")
		return nil, err
	}
	shardClients := haxmap.New[string, *ShardClient]()
	for shardId, shard := range clusterInfo.Info.ShardMap {
		shardClient := ShardClient{
			leader:        shard.LeaderId,
			memberclients: make([]*MemberClient, 10),
		}
		shardClients.Set(shardId, &shardClient)
		for nodeId, member := range shard.MemberAddressMap {
			con, err = newClientConnection(member.Address)
			if err != nil {
				log.Err(err).Msgf("Error creating connection")
				return nil, err
			}
			memberClient := MemberClient{
				nodeId:        nodeId,
				clusterClient: newClusterClient(con),
				messageClient: newMessageClient(con),
			}
			shardClient.memberclients = append(shardClient.memberclients, &memberClient)
		}
	}

	return &JetClient{
		info:         clusterInfo.Info,
		shardClients: shardClients,
	}, nil
}

func PublishMessage(messages []*proto.Message) (*proto.PublishMessageResponse, error) {
	return nil, nil
}

func CreateTopic(name string, partitions int) (*proto.CreateTopicResponse, error) {
	return nil, nil
}

func CreateConsumer(id uint64) (*proto.CreateConsumerResponse, error) {
	return nil, nil
}

func ConsumeMessage(topic string, id uint64) (*proto.ConsumeRequest, error) {
	return nil, nil
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
