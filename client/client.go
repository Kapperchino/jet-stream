package client

import (
	"context"
	"errors"
	"github.com/Kapperchino/jet-application/proto"
	clusterPb "github.com/Kapperchino/jet-cluster/proto"
	"github.com/alphadose/haxmap"
	"github.com/buraksezer/consistent"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/rs/zerolog/log"
	"github.com/spaolacci/murmur3"
	"google.golang.org/grpc"
	"math/rand"
	"strconv"
	"time"
)

type JetClient struct {
	info         *clusterPb.ClusterInfo
	shardClients *haxmap.Map[string, *ShardClient]
	metaData     *Meta
}

type Meta struct {
	topics *haxmap.Map[string, *TopicMeta]
}

type TopicMeta struct {
	partitions *haxmap.Map[uint64, *PartitionMeta]
	hash       *consistent.Consistent
}

type PartitionMeta struct {
	partitionNum uint64
	topic        string
	shardId      string
}

type ShardClient struct {
	leader          string
	memberclients   *haxmap.Map[string, *MemberClient]
	roundRobinIndex int32
	partitions      *haxmap.Map[uint64, *PartitionMeta]
}

type MemberClient struct {
	nodeId        string
	clusterClient clusterPb.ClusterMetaServiceClient
	messageClient proto.MessageServiceClient
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return murmur3.Sum64(data)
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
			memberclients: haxmap.New[string, *MemberClient](),
			partitions:    haxmap.New[uint64, *PartitionMeta](),
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
			shardClient.memberclients.Set(nodeId, &memberClient)
		}
	}
	metaData := Meta{topics: haxmap.New[string, *TopicMeta]()}
	shardClients.ForEach(func(shardId string, client *ShardClient) bool {
		meta, err := client.GetNextMember().messageClient.GetMeta(ctx, &proto.GetMetaRequest{})
		if err != nil {
			log.Err(err).Msgf("Error while getting meta from shard %s", shardId)
			return false
		}
		for key, topic := range meta.Topics {
			//updating the map
			var item, exists = metaData.topics.Get(key)
			if !exists {
				cfg := consistent.Config{
					PartitionCount:    7,
					ReplicationFactor: 1,
					Load:              1.25,
					Hasher:            hasher{},
				}
				metaData.topics.Set(key, &TopicMeta{
					partitions: haxmap.New[uint64, *PartitionMeta](),
					hash:       consistent.New(nil, cfg),
				})
			}
			partitions := item.partitions
			for num, partition := range topic.Partitions {
				partitionMeta := &PartitionMeta{
					partitionNum: num,
					topic:        partition.Topic,
					shardId:      shardId,
				}
				item.hash.Add(partitionMeta)
				partitions.Set(num, partitionMeta)
				client.partitions.Set(num, partitionMeta)
			}
		}
		return true
	},
	)

	return &JetClient{
		info:         clusterInfo.Info,
		shardClients: shardClients,
		metaData:     &metaData,
	}, nil
}

// PublishMessage need to get each partition's location within the cluster, then do consistent hashing with the key to get the partition needed
// TODO balancing
func (j *JetClient) PublishMessage(messages []*proto.KeyVal, topic string) (*proto.PublishMessageResponse, error) {
	meta, exists := j.metaData.topics.Get(topic)
	if !exists {
		return nil, errors.New("topic does not exist")
	}
	//get partition to publish to
	bucket := map[uint64][]*proto.KeyVal{}
	for _, message := range messages {
		partition, err := strconv.ParseUint(meta.hash.LocateKey(message.Key).String(), 10, 64)
		if err != nil {
			return nil, err
		}
		bucket[partition] = append(bucket[partition], message)
	}
	var resList []*proto.PublishMessageResponse
	for partition, list := range bucket {
		meta, _ := meta.partitions.Get(partition)
		client, _ := j.shardClients.Get(meta.shardId)
		res, err := client.GetLeader().messageClient.PublishMessages(context.Background(), &proto.PublishMessageRequest{
			Topic:     topic,
			Partition: partition,
			Messages:  list,
		})
		if err != nil {
			return nil, err
		}
		resList = append(resList, res)
	}
	var msgList []*proto.Message
	for _, response := range resList {
		msgList = append(msgList, response.Messages...)
	}
	return &proto.PublishMessageResponse{
		Messages:  msgList,
		LastIndex: 0,
	}, nil
}

type KeyValuePair struct {
	id    string
	count int
}

func (j *JetClient) CreateTopic(name string, partitions int) (*proto.CreateTopicResponse, error) {
	_, exists := j.metaData.topics.Get(name)
	if exists {
		return nil, errors.New("topic exists")
	}
	var ids []string
	j.shardClients.ForEach(func(s string, client *ShardClient) bool {
		ids = append(ids, s)
		return true
	})
	//round robind
	curIndex := 0
	partitionNum := 0
	var partitionsToCreate [][]uint64
	j.shardClients.ForEach(func(s string, client *ShardClient) bool {
		partitionsToCreate = append(partitionsToCreate, []uint64{})
		return true
	})
	for partitionNum < partitions {
		partitionsToCreate[curIndex] = append(partitionsToCreate[curIndex], uint64(partitionNum))
		partitionNum++
		curIndex = (curIndex + 1) % len(partitionsToCreate)
	}
	curIndex = 0
	j.shardClients.ForEach(func(s string, client *ShardClient) bool {
		req := proto.CreateTopicRequest{
			Topic:      name,
			Partitions: partitionsToCreate[curIndex],
		}
		_, err := client.GetLeader().messageClient.CreateTopic(context.Background(), &req)
		if err != nil {
			log.Err(err).Msgf("Error creating topic")
			return false
		}
		return true
	})
	return &proto.CreateTopicResponse{}, nil
}

// CreateConsumerGroup creates multiple consumers on each shard that contains the partitions of the topic, stores
// the id of each consumer
func (j *JetClient) CreateConsumerGroup(topic string) (*proto.CreateConsumerResponse, error) {
	return nil, nil
}

// ConsumeMessage need to check if the consumer is created, if true then find
func (j *JetClient) ConsumeMessage(topic string, id uint64) (*proto.ConsumeRequest, error) {
	return nil, nil
}

func (s *ShardClient) GetLeader() *MemberClient {
	res, _ := s.memberclients.Get(s.leader)
	return res
}

func (s *ShardClient) GetNextMember() *MemberClient {
	if s.memberclients.Len() == 1 {
		var res *MemberClient
		s.memberclients.ForEach(func(s string, client *MemberClient) bool {
			res = client
			return true
		})
		return res
	}
	var memberClients []*MemberClient
	s.memberclients.ForEach(func(id string, client *MemberClient) bool {
		if id != s.leader {
			memberClients = append(memberClients, client)
		}
		return true
	})
	index := rand.Int() % len(memberClients)
	return memberClients[index]
}

func (p *PartitionMeta) String() string {
	return strconv.FormatUint(p.partitionNum, 10)
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
