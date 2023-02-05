package client

import (
	"context"
	"github.com/Kapperchino/jet-application/proto"
	clusterPb "github.com/Kapperchino/jet-cluster/proto"
	"github.com/alphadose/haxmap"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
	"github.com/rs/zerolog/log"
	"math/rand"
	"strconv"
)

type JetClient struct {
	info         *clusterPb.ClusterInfo
	shardClients *haxmap.Map[string, *ShardClient]
	metaData     *Meta
}

type ConsumerGroup struct {
	group *proto.ConsumerGroup
}

type Meta struct {
	topics         *haxmap.Map[string, *TopicMeta]
	consumerGroups *haxmap.Map[string, *ConsumerGroup]
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
	shardId         string
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
	return xxhash.Sum64(data)
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
	metaData := Meta{topics: haxmap.New[string, *TopicMeta](), consumerGroups: haxmap.New[string, *ConsumerGroup]()}
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
				metaData.topics.Set(key, &TopicMeta{
					partitions: haxmap.New[uint64, *PartitionMeta](),
					hash:       newHashRing(),
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
