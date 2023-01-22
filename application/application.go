package application

import (
	"context"
	"errors"
	"github.com/Kapperchino/jet-application/fsm"
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/Kapperchino/jet-application/util"
	"github.com/Kapperchino/jet-leader-rpc/rafterrors"
	"github.com/hashicorp/raft"
	"time"
)

type RpcInterface struct {
	NodeState *fsm.NodeState
	Raft      *raft.Raft
	pb.UnimplementedExampleServer
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

func CreateConsumerInternal(r RpcInterface, req *pb.CreateConsumerRequest) (*pb.CreateConsumerResponse, error) {
	input := &pb.WriteOperation{
		Operation: &pb.WriteOperation_CreateConsumer{
			CreateConsumer: &pb.CreateConsumer{
				Topic: req.GetTopic(),
			},
		},
		Code: pb.Operation_CREATE_CONSUMER,
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
	response, isValid := res.Response().(*pb.CreateConsumerResponse)
	if !isValid {
		return nil, errors.New("unknown data type")
	}
	return response, nil
}

func (r RpcInterface) CreateConsumer(ctx context.Context, req *pb.CreateConsumerRequest) (*pb.CreateConsumerResponse, error) {
	res, err := CreateConsumerInternal(r, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func ConsumeInternal(r RpcInterface, req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	input := &pb.WriteOperation{
		Operation: &pb.WriteOperation_Consume{
			Consume: &pb.Consume{
				Topic: req.GetTopic(),
				Id:    req.GetConsumerId(),
			},
		},
		Code: pb.Operation_CONSUME,
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
	response, isValid := res.Response().(*pb.ConsumeResponse)
	if !isValid {
		return nil, errors.New("unknown data type")
	}
	return response, nil
}

func (r RpcInterface) Consume(_ context.Context, req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	res, err := ConsumeInternal(r, req)
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
				Partitions: req.GetNumPartitions(),
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
