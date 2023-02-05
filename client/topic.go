package client

import (
	"context"
	"errors"
	"github.com/Kapperchino/jet-application/proto"
	"github.com/alphadose/haxmap"
	"github.com/rs/zerolog/log"
)

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

	hash := newHashRing()
	curIndex = 0
	var err error
	partitionsMeta := haxmap.New[uint64, *PartitionMeta]()
	j.shardClients.ForEach(func(s string, client *ShardClient) bool {
		partitionList := partitionsToCreate[curIndex]
		curIndex++
		req := proto.CreateTopicRequest{
			Topic:      name,
			Partitions: partitionList,
		}
		_, err = client.GetLeader().messageClient.CreateTopic(context.Background(), &req)
		if err != nil {
			log.Err(err).Msgf("Error creating topic")
			return false
		}
		for _, num := range partitionList {
			meta := &PartitionMeta{
				partitionNum: num,
				topic:        name,
				shardId:      client.shardId,
			}
			partitionsMeta.Set(num, meta)
			client.partitions.Set(num, meta)
			hash.Add(meta)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	j.metaData.topics.Set(name, &TopicMeta{
		partitions: partitionsMeta,
		hash:       hash,
	})
	return &proto.CreateTopicResponse{}, nil
}
