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
func (j *JetClient) CreateConsumerGroup(topicName string) (*proto.CreateConsumerGroupResponse, error) {
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
	slice := partitionSet.ToSlice()
	for _, s := range slice {
		client, exist := j.shardClients.Get(s)
		if !exist {
			curErr = errors.New("shard needs to be in the meta")
			return nil, curErr
		}
		group, err := client.GetLeader().messageClient.CreateConsumerGroup(context.Background(), &proto.CreateConsumerGroupRequest{
			Topic: topicName,
			Id:    id,
		})
		if err != nil {
			curErr = err
			log.Error().Stack().Err(err)
			return nil, err
		}
		j.metaData.consumerGroups.Set(group.Id, &ConsumerGroup{
			group: group.Group,
		})
	}
	return &proto.CreateConsumerGroupResponse{
		Id:    id,
		Group: nil,
	}, nil
}

// ConsumeMessage need to check if the consumer is created, if true then find
func (j *JetClient) ConsumeMessage(topicName string, id string) ([]*proto.Message, error) {
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
	var combinedRes []*proto.Message
	slice := partitionSet.ToSlice()
	for _, s := range slice {
		client, exist := j.shardClients.Get(s)
		clients = append(clients, client)
		if !exist {
			curErr = errors.New("shard needs to be in the meta")
			return nil, curErr
		}
		res, err := client.GetNextMember().messageClient.Consume(context.Background(), &proto.ConsumeRequest{
			Topic:   topicName,
			GroupId: id,
		})
		if err != nil {
			log.Err(err).Stack().Msgf("Error consuming from group %s", id)
			return nil, err
		}
		if len(res.Messages) == 0 {
			continue
		}
		for p, messages := range res.Messages {
			if messages.Messages == nil {
				continue
			}
			lastMsg := messages.Messages[len(messages.Messages)-1]
			partitionMap[p] = lastMsg.Offset
			combinedRes = append(combinedRes, messages.Messages...)
		}
		if err != nil {
			curErr = err
			return nil, curErr
		}
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
