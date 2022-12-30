package application

import (
	"context"
	"github.com/Kapperchino/jet-application/fsm"
	"github.com/Kapperchino/jet-application/util"
	"github.com/Kapperchino/jet-leader-rpc/rafterrors"
	pb "github.com/Kapperchino/jet/proto"
	"github.com/hashicorp/raft"
	"time"
)

type RpcInterface struct {
	NodeState *fsm.NodeState
	Raft      *raft.Raft
	pb.UnimplementedExampleServer
}

func PublishMessagesInternal(r RpcInterface, req *pb.PublishMessageRequest) ([]*pb.Message, error) {
	input := &pb.Write{
		Operation: &pb.Write_Publish{
			Publish: &pb.Publish{
				Topic:    req.GetTopic(),
				Messages: req.GetMessages(),
			},
		},
	}
	val, _ := util.SerializeMessage(input)
	res := r.Raft.Apply(val, time.Second)
	if err := res.Error(); err != nil {
		return nil, rafterrors.MarkRetriable(err)
	}
	return res.Response().([]*pb.Message), nil
}

func (r RpcInterface) PublishMessages(ctx context.Context, req *pb.PublishMessageRequest) (*pb.PublishMessageResponse, error) {
	messages, err := PublishMessagesInternal(r, req)
	res := &pb.PublishMessageResponse{Messages: messages}
	if err != nil {
		return nil, err
	}
	return res, nil
}

func CreateConsumerInternal(r RpcInterface, req *pb.CreateConsumerRequest) (*pb.CreateConsumerResponse, error) {
	input := &pb.Write{
		Operation: &pb.Write_CreateConsumer{
			CreateConsumer: &pb.CreateConsumer{
				Topic: req.GetTopic(),
			},
		},
	}
	val, _ := util.SerializeMessage(input)
	res := r.Raft.Apply(val, time.Second)
	if err := res.Error(); err != nil {
		return nil, rafterrors.MarkRetriable(err)
	}
	return res.Response().(*pb.CreateConsumerResponse), nil
}

func (r RpcInterface) CreateConsumer(ctx context.Context, req *pb.CreateConsumerRequest) (*pb.CreateConsumerResponse, error) {
	res, err := CreateConsumerInternal(r, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func ConsumeInternal(r RpcInterface, req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	input := &pb.Write{
		Operation: &pb.Write_Consume{
			Consume: &pb.Consume{
				Topic: req.GetTopic(),
				Id:    req.GetConsumerId(),
			},
		},
	}
	val, _ := util.SerializeMessage(input)
	res := r.Raft.Apply(val, time.Second)
	if err := res.Error(); err != nil {
		return nil, rafterrors.MarkRetriable(err)
	}
	return res.Response().(*pb.ConsumeResponse), nil
}

func (r RpcInterface) Consume(_ context.Context, req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	res, err := ConsumeInternal(r, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func CreateTopicInternal(r RpcInterface, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	input := &pb.Write{
		Operation: &pb.Write_CreateTopic{
			CreateTopic: &pb.CreateTopic{
				Topic:      req.GetTopic(),
				Partitions: req.GetNumPartitions(),
			},
		},
	}
	val, _ := util.SerializeMessage(input)
	res := r.Raft.Apply(val, time.Second)
	if err := res.Error(); err != nil {
		return nil, rafterrors.MarkRetriable(err)
	}
	return res.Response().(*pb.CreateTopicResponse), nil
}

func (r RpcInterface) CreateTopic(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	res, err := CreateTopicInternal(r, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}
