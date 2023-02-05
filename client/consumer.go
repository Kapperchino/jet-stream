package client

import (
	"context"
	"errors"
	"github.com/Kapperchino/jet-application/proto"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// CreateConsumerGroup creates multiple consumers on each shard that contains the partitions of the topic, stores
// the id of each consumer
func (j *JetClient) CreateConsumerGroup(topicName string) (*proto.CreateConsumerResponse, error) {
	topic, exist := j.metaData.topics.Get(topicName)
	if !exist {
		return nil, errors.New("topic does not exist")
	}
	partitionSet := mapset.NewSet[string]()
	topic.partitions.ForEach(func(u uint64, meta *PartitionMeta) bool {
		partitionSet.Add(meta.shardId)
		return true
	})
	var curErr error
	id := uuid.NewString()
	partitionSet.Each(func(s string) bool {
		client, exist := j.shardClients.Get(s)
		if !exist {
			curErr = errors.New("shard needs to be in the meta")
			return false
		}
		group, err := client.GetLeader().messageClient.CreateConsumerGroup(context.Background(), &proto.CreateConsumerGroupRequest{
			Topic: topicName,
			Id:    id,
		})
		if err != nil {
			curErr = err
			log.Error().Stack().Err(err)
			return false
		}
		j.metaData.consumerGroups.Set(group.Id, &ConsumerGroup{
			group: group.Group,
		})
		return true
	})
	if curErr != nil {
		return nil, curErr
	}
	return &proto.CreateConsumerResponse{}, nil
}

// ConsumeMessage need to check if the consumer is created, if true then find
func (j *JetClient) ConsumeMessage(topicName string, id string) (*proto.ConsumeResponse, error) {
	_, exist := j.metaData.consumerGroups.Get(id)
	if !exist {
		return nil, errors.New("group does not exist")
	}
	topic, exist := j.metaData.topics.Get(topicName)
	if !exist {
		return nil, errors.New("topic does not exist")
	}
	partitionSet := mapset.NewSet[string]()
	topic.partitions.ForEach(func(u uint64, meta *PartitionMeta) bool {
		partitionSet.Add(meta.shardId)
		return true
	})
	var curErr error
	partitionMap := map[uint64]uint64{}
	var clients []*ShardClient
	combinedRes := &proto.ConsumeResponse{
		Messages:  []*proto.Message{},
		LastIndex: 0,
	}
	partitionSet.Each(func(s string) bool {
		client, exist := j.shardClients.Get(s)
		clients = append(clients, client)
		if !exist {
			curErr = errors.New("shard needs to be in the meta")
			return false
		}
		res, err := client.GetNextMember().messageClient.Consume(context.Background(), &proto.ConsumeRequest{
			Topic:   topicName,
			GroupId: id,
		})
		msgs := res.Messages
		lastMsg := msgs[len(msgs)-1]
		partitionMap[lastMsg.Partition] = lastMsg.Offset
		if err != nil {
			curErr = err
			return false
		}
		combinedRes.Messages = append(combinedRes.Messages, msgs...)
		return true
	})
	if curErr != nil {
		return nil, curErr
	}

	//ack everything
	for _, client := range clients {
		_, err := client.GetLeader().messageClient.AckConsume(context.Background(), &proto.AckConsumeRequest{
			Offsets: partitionMap,
			GroupId: id,
			Topic:   topicName,
		})
		if err != nil {
			return nil, err
		}
	}
	return combinedRes, nil
}
