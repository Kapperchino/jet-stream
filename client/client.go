package client

import (
	"context"
	proto "github.com/Kapperchino/jet-stream/application/proto/proto"
	clusterPb "github.com/Kapperchino/jet-stream/cluster/proto/proto"
	"github.com/Kapperchino/jet-stream/util"
	"github.com/buraksezer/consistent"
	"github.com/rs/zerolog/log"
	"github.com/spaolacci/murmur3"
	"math/rand"
	"strconv"
)

type JetClient struct {
	info         *clusterPb.ClusterInfo
	shardClients *util.Map[string, *ShardClient]
	metaData     *Meta
}

type ConsumerGroup struct {
	group *proto.ConsumerGroup
}

type Meta struct {
	topics         *util.Map[string, *TopicMeta]
	consumerGroups *util.Map[string, *ConsumerGroup]
}

type TopicMeta struct {
	partitions *util.Map[uint64, *PartitionMeta]
	hash       *consistent.Consistent
}

type PartitionMeta struct {
	partitionNum uint64
	topic        string
	shardId      string
}

type ShardClient struct {
	leader          string
	shardId         string
	memberclients   *util.Map[string, *MemberClient]
	roundRobinIndex int32
	partitions      *util.Map[uint64, *PartitionMeta]
	buffer          [100]*proto.KeyVal
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
	shardClients := util.NewMap[string, *ShardClient]()
	for shardId, shard := range clusterInfo.Info.ShardMap {
		shardClient := ShardClient{
			leader:        shard.LeaderId,
			memberclients: util.NewMap[string, *MemberClient](),
			partitions:    util.NewMap[uint64, *PartitionMeta](),
			shardId:       shardId,
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
	metaData := Meta{topics: util.NewMap[string, *TopicMeta](), consumerGroups: util.NewMap[string, *ConsumerGroup]()}
	shardClients.ForEach(func(shardId string, client *ShardClient) bool {
		meta, err := client.GetNextMember().messageClient.GetMeta(ctx, &proto.GetMetaRequest{})
		if err != nil {
			log.Err(err).Msgf("Error while getting meta from shard %s", shardId)
			return false
		}
		for key, topic := range meta.Topics {
			//updating the map
			var item = metaData.topics.Get(key)
			if item == nil {
				topicMeta := &TopicMeta{
					partitions: util.NewMap[uint64, *PartitionMeta](),
					hash:       newHashRing(),
				}
				for _, partition := range topic.Partitions {
					topicMeta.partitions.Set(partition.Num, &PartitionMeta{
						partitionNum: partition.Num,
						topic:        topic.Name,
						shardId:      shardId,
					})
				}
				metaData.topics.Set(key, topicMeta)
				item = topicMeta
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

		for id, group := range meta.ConsumerGroups {
			var partitions []uint64
			for _, consumer := range group.Consumers {
				partitions = append(partitions, consumer.Partition)
			}
			metaData.consumerGroups.Set(id, &ConsumerGroup{
				group: group,
			})
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

type KeyValuePair struct {
	id    string
	count int
}

func (s *ShardClient) GetLeader() *MemberClient {
	res := s.memberclients.Get(s.leader)
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
