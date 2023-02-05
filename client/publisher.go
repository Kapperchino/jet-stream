package client

import (
	"context"
	"errors"
	"github.com/Kapperchino/jet-application/proto"
	"strconv"
)

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
