package client

import (
	"context"
	"errors"
	"github.com/Kapperchino/jet-stream/application/proto/proto"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"strconv"
)

// PublishMessage need to get each partition's location within the cluster, then do consistent hashing with the key to get the partition needed
func (j *JetClient) PublishMessage(messages []*proto.KeyVal, topic string) (*proto.PublishMessageResponse, error) {
	meta := j.metaData.topics.Get(topic)
	if meta == nil {
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
	publishGroup, _ := errgroup.WithContext(context.Background())
	resChannel := make(chan *proto.PublishMessageResponse, len(bucket))
	for partition, list := range bucket {
		partition := partition
		meta := meta.partitions.Get(partition)
		list := list
		client := j.shardClients.Get(meta.shardId)
		messageClient := client.GetLeader().messageClient
		publishGroup.Go(func() error {
			log.Debug().Msgf("Client publishing batch size of %v to partition: %v", len(list), partition)
			return PublishMessages(messageClient, topic, partition, list, resChannel)
		})
	}
	err := publishGroup.Wait()
	if err != nil {
		return nil, err
	}
	close(resChannel)
	var msgList []*proto.Message
	for response := range resChannel {
		msgList = append(msgList, response.Messages...)
	}
	return &proto.PublishMessageResponse{
		Messages:  msgList,
		LastIndex: 0,
	}, nil
}

func PublishMessages(client proto.MessageServiceClient, topicName string, partition uint64, list []*proto.KeyVal, channel chan *proto.PublishMessageResponse) error {
	res, err := client.PublishMessages(context.Background(), &proto.PublishMessageRequest{
		Topic:     topicName,
		Partition: partition,
		Messages:  list,
	})
	if err != nil {
		log.Err(err).Stack().Msgf("Error publishing to topic %v,partition %v", topicName, partition)
		return err
	}
	channel <- res
	return nil
}
