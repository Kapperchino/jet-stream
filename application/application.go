package application

import (
	"context"
	"errors"
	"github.com/Kapperchino/jet-application/fsm"
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/Kapperchino/jet-leader-rpc/rafterrors"
	"github.com/Kapperchino/jet/util"
	"github.com/hashicorp/raft"
	"time"
)

type RpcInterface struct {
	NodeState *fsm.NodeState
	Raft      *raft.Raft
	pb.UnimplementedMessageServiceServer
}

func PublishMessagesInternal(r RpcInterface, req *pb.PublishMessageRequest) ([]*pb.Message, error) {
	input := &pb.WriteOperation{
		Operation: &pb.WriteOperation_Publish{
			Publish: &pb.Publish{
				Topic:     req.GetTopic(),
				Partition: req.GetPartition(),
				Messages:  req.GetMessages(),
			},
		},
		Code: pb.Operation_PUBLISH,
	}
	val, _ := util.SerializeMessage(input)
	res := r.Raft.Apply(val, time.Second)
	if err := res.Error(); err != nil {
		return nil, rafterrors.MarkRetriable(err)
	}
	err, isErr := res.Response().(error)
	if isErr {
		return nil, err
	}
	response, isValid := res.Response().([]*pb.Message)
	if !isValid {
		return nil, errors.New("unknown data type")
	}
	return response, nil
}

func (r RpcInterface) PublishMessages(ctx context.Context, req *pb.PublishMessageRequest) (*pb.PublishMessageResponse, error) {
	messages, err := PublishMessagesInternal(r, req)
	res := &pb.PublishMessageResponse{Messages: messages}
	r.Raft.LastIndex()
	if err != nil {
		return nil, err
	}
	return res, nil
}

func ConsumeInternal(r RpcInterface, req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	return r.NodeState.Consume(req)
}

func (r RpcInterface) Consume(_ context.Context, req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	res, err := ConsumeInternal(r, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func GetConsumerGroupsInternal(r RpcInterface, req *pb.GetConsumerGroupsRequest) (*pb.GetConsumerGroupsResponse, error) {
	return r.NodeState.GetConsumerGroups(req.Topic)
}

func (r RpcInterface) GetConsumerGroups(_ context.Context, req *pb.GetConsumerGroupsRequest) (*pb.GetConsumerGroupsResponse, error) {
	res, err := GetConsumerGroupsInternal(r, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func GetMetaInternal(r RpcInterface, req *pb.GetMetaRequest) (*pb.GetMetaResponse, error) {
	return r.NodeState.GetMeta()
}

func (r RpcInterface) GetMeta(_ context.Context, req *pb.GetMetaRequest) (*pb.GetMetaResponse, error) {
	res, err := GetMetaInternal(r, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func CreateTopicInternal(r RpcInterface, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	input := &pb.WriteOperation{
		Operation: &pb.WriteOperation_CreateTopic{
			CreateTopic: &pb.CreateTopic{
				Topic:      req.GetTopic(),
				Partitions: req.GetPartitions(),
			},
		},
		Code: pb.Operation_CREATE_TOPIC,
	}
	val, _ := util.SerializeMessage(input)
	res := r.Raft.Apply(val, time.Second)
	if err := res.Error(); err != nil {
		return nil, rafterrors.MarkRetriable(err)
	}
	err, isErr := res.Response().(error)
	if isErr {
		return nil, err
	}
	response, isValid := res.Response().(*pb.CreateTopicResponse)
	if !isValid {
		return nil, errors.New("unknown data type")
	}
	return response, nil
}

func (r RpcInterface) CreateTopic(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	res, err := CreateTopicInternal(r, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (r RpcInterface) AckConsume(_ context.Context, req *pb.AckConsumeRequest) (*pb.AckConsumeResponse, error) {
	res, err := AckConsumeInternal(r, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func AckConsumeInternal(r RpcInterface, req *pb.AckConsumeRequest) (*pb.AckConsumeResponse, error) {
	input := &pb.WriteOperation{
		Operation: &pb.WriteOperation_Ack{
			Ack: &pb.Ack{
				Offsets: req.Offsets,
				GroupId: req.GroupId,
				Topic:   req.Topic,
			},
		},
		Code: pb.Operation_ACK,
	}
	val, _ := util.SerializeMessage(input)
	res := r.Raft.Apply(val, time.Second)
	if err := res.Error(); err != nil {
		return nil, rafterrors.MarkRetriable(err)
	}
	err, isErr := res.Response().(error)
	if isErr {
		return nil, err
	}
	response, isValid := res.Response().(*pb.AckConsumeResponse)
	if !isValid {
		return nil, errors.New("unknown data type")
	}
	return response, nil
}

func (r RpcInterface) CreateConsumerGroup(_ context.Context, req *pb.CreateConsumerGroupRequest) (*pb.CreateConsumerGroupResponse, error) {
	res, err := CreateConsumerGroupInternal(r, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func CreateConsumerGroupInternal(r RpcInterface, req *pb.CreateConsumerGroupRequest) (*pb.CreateConsumerGroupResponse, error) {
	input := &pb.WriteOperation{
		Operation: &pb.WriteOperation_CreateConsumerGroup{
			CreateConsumerGroup: &pb.CreateConsumerGroup{
				Topic: req.Topic,
			},
		},
		Code: pb.Operation_CREATE_CONSUMER_GROUP,
	}
	val, _ := util.SerializeMessage(input)
	res := r.Raft.Apply(val, time.Second)
	if err := res.Error(); err != nil {
		return nil, rafterrors.MarkRetriable(err)
	}
	err, isErr := res.Response().(error)
	if isErr {
		return nil, err
	}
	response, isValid := res.Response().(*pb.CreateConsumerGroupResponse)
	if !isValid {
		return nil, errors.New("unknown data type")
	}
	return response, nil
}
