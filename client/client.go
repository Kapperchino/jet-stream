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

// PublishMessage need to get each partition's location within the cluster, then do consistent hashing with the key to get the partition needed
// TODO balancing
func PublishMessage(messages []*proto.KeyVal, topic string) (*proto.PublishMessageResponse, error) {
	return nil, nil
}

// CreateTopic needs to first check if the topic already exists, and if not it'll need to distribute partitions evenly across
// shards
func CreateTopic(name string, partitions int) (*proto.CreateTopicResponse, error) {
	return nil, nil
}

// CreateConsumerGroup creates multiple consumers on each shard that contains the partitions of the topic, stores
// the id of each consumer
func CreateConsumerGroup(topic string) (*proto.CreateConsumerResponse, error) {
	return nil, nil
}

// ConsumeMessage need to check if the consumer is created, if true then find
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
