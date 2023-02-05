package client

import "github.com/Kapperchino/jet-application/proto"

// CreateConsumerGroup creates multiple consumers on each shard that contains the partitions of the topic, stores
// the id of each consumer
func (j *JetClient) CreateConsumerGroup(topic string) (*proto.CreateConsumerResponse, error) {
	return nil, nil
}

// ConsumeMessage need to check if the consumer is created, if true then find
func (j *JetClient) ConsumeMessage(topic string, id uint64) (*proto.ConsumeRequest, error) {
	return nil, nil
}
