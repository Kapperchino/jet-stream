package client

import (
	"context"
	"errors"
	"github.com/Kapperchino/jet-stream/application/proto/proto"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// CreateConsumerGroup creates multiple consumers on each shard that contains the partitions of the topic, stores
// the id of each consumer
func (j *JetClient) CreateConsumerGroup(topicName string) (*proto.CreateConsumerGroupResponse, error) {
	topic := j.metaData.topics.Get(topicName)
	if topic == nil {
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
		client := j.shardClients.Get(s)
		if client == nil {
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
	val := j.metaData.consumerGroups.Get(id)
	if val == nil {
		return nil, errors.New("group does not exist")
	}
	offsets := map[uint64]uint64{}
	for _, consumer := range val.group.Consumers {
		offsets[consumer.Partition] = consumer.Offset
	}
	topic := j.metaData.topics.Get(topicName)
	if topic == nil {
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
	consumeGroup, _ := errgroup.WithContext(context.Background())
	resChannel := make(chan *proto.ConsumeResponse, len(slice))
	for _, s := range slice {
		client := j.shardClients.Get(s)
		clients = append(clients, client)
		if client == nil {
			curErr = errors.New("shard needs to be in the meta")
			return nil, curErr
		}
		curClient := client.GetNextMember().messageClient
		consumeGroup.Go(func() error {
			return ConsumeMessages(curClient, topicName, id, resChannel, offsets)
		})
	}
	err := consumeGroup.Wait()
	close(resChannel)
	if err != nil {
		log.Err(err).Stack().Msgf("Error consuming from group %s", id)
		return nil, err
	}
	for res := range resChannel {
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
	}
	if len(combinedRes) == 0 {
		return combinedRes, nil
	}

	ackGroup, _ := errgroup.WithContext(context.Background())
	ackChannel := make(chan *proto.AckConsumeResponse, len(slice))
	//ack everything
	for _, client := range clients {
		client := client.GetLeader().messageClient
		ackGroup.Go(func() error {
			return AckMessages(client, topicName, id, partitionMap, ackChannel)
		})
		if err != nil {
			return nil, err
		}
	}
	err = ackGroup.Wait()
	close(ackChannel)
	if err != nil {
		log.Err(err).Stack().Msgf("Error consuming from group %s", id)
		return nil, err
	}

	for _, consumer := range val.group.Consumers {
		consumer.Offset = partitionMap[consumer.Partition]
	}
	return combinedRes, nil
}

func ConsumeMessages(client proto.MessageServiceClient, topicName string, id string, channel chan *proto.ConsumeResponse, offsets map[uint64]uint64) error {
	res, err := client.Consume(context.Background(), &proto.ConsumeRequest{
		Topic:   topicName,
		GroupId: id,
		Offsets: offsets,
	})
	if err != nil {
		log.Err(err).Stack().Msgf("Error consuming from group %s", id)
		return err
	}
	channel <- res
	return nil
}

func AckMessages(client proto.MessageServiceClient, topicName string, id string, partitionMap map[uint64]uint64, channel chan *proto.AckConsumeResponse) error {
	res, err := client.AckConsume(context.Background(), &proto.AckConsumeRequest{
		Offsets: partitionMap,
		GroupId: id,
		Topic:   topicName,
	})
	if err != nil {
		log.Err(err).Stack().Msgf("Error consuming from group %s", id)
		return err
	}
	channel <- res
	return nil
}
