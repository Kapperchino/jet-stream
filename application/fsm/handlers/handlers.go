package handlers

import (
	"github.com/Kapperchino/jet-application/fsm"
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"
)

func InitHandlers() []func(f *fsm.NodeState, op *pb.WriteOperation, l *raft.Log) interface{} {
	handlerMap := make([]func(f *fsm.NodeState, op *pb.WriteOperation, l *raft.Log) interface{}, 100)
	handlerMap[pb.Operation_PUBLISH] = HandlePublish
	handlerMap[pb.Operation_ACK] = HandleAck
	handlerMap[pb.Operation_CREATE_TOPIC] = HandleCreateTopic
	handlerMap[pb.Operation_ADD_MEMBER] = HandleAddMember
	handlerMap[pb.Operation_REMOVE_MEMBER] = HandleRemoveMember
	handlerMap[pb.Operation_CREATE_CONSUMER_GROUP] = HandleCreateConsumerGroup
	return handlerMap
}

func HandleAck(f *fsm.NodeState, op *pb.WriteOperation, l *raft.Log) interface{} {
	res, err := f.Ack(op.GetAck())
	if err != nil {
		log.Error().Err(err)
		return err
	}
	return res
}

func HandleRemoveMember(f *fsm.NodeState, op *pb.WriteOperation, l *raft.Log) interface{} {
	res, err := f.RemoveMember(op.GetRemoveMember())
	if err != nil {
		log.Error().Err(err)
		return err
	}
	return res
}

func HandleAddMember(f *fsm.NodeState, op *pb.WriteOperation, l *raft.Log) interface{} {
	res, err := f.AddMember(op.GetAddMember())
	if err != nil {
		log.Error().Err(err)
		return err
	}
	return res
}

func HandlePublish(f *fsm.NodeState, op *pb.WriteOperation, l *raft.Log) interface{} {
	res, err := f.Publish(op.GetPublish(), l.Index)
	if err != nil {
		log.Error().Err(err)
		return err
	}
	return res
}

func HandleCreateTopic(f *fsm.NodeState, op *pb.WriteOperation, l *raft.Log) interface{} {
	res, err := f.CreateTopic(op.GetCreateTopic())
	if err != nil {
		log.Error().Err(err)
		return err
	}
	return res
}

func HandleCreateConsumerGroup(f *fsm.NodeState, op *pb.WriteOperation, l *raft.Log) interface{} {
	res, err := f.CreateConsumerGroup(op.GetCreateConsumerGroup())
	if err != nil {
		log.Error().Err(err)
		return err
	}
	return res
}
