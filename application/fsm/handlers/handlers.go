package handlers

import (
	"github.com/Kapperchino/jet-application/fsm"
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"
)

func InitHandlers() map[pb.Operation]func(f *fsm.NodeState, op *pb.WriteOperation, l *raft.Log) interface{} {
	handlerMap := make(map[pb.Operation]func(f *fsm.NodeState, op *pb.WriteOperation, l *raft.Log) interface{})
	handlerMap[pb.Operation_PUBLISH] = HandlePublish
	handlerMap[pb.Operation_CONSUME] = HandleConsume
	handlerMap[pb.Operation_CREATE_CONSUMER] = HandleCreateConsume
	handlerMap[pb.Operation_CREATE_TOPIC] = HandleCreateTopic
	handlerMap[pb.Operation_ADD_MEMBER] = HandleAddMember
	return handlerMap
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

func HandleConsume(f *fsm.NodeState, op *pb.WriteOperation, l *raft.Log) interface{} {
	res, err := f.Consume(op.GetConsume())
	if err != nil {
		log.Error().Err(err)
		return err
	}
	return res
}

func HandleCreateConsume(f *fsm.NodeState, op *pb.WriteOperation, l *raft.Log) interface{} {
	res, err := f.CreateConsumer(op.GetCreateConsumer())
	if err != nil {
		log.Error().Err(err)
		return err
	}
	return res
}
