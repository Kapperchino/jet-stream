package fsm

import (
	pb "github.com/Kapperchino/jet-stream/application/proto/proto"
	"github.com/Kapperchino/jet-stream/util"
	"github.com/dgraph-io/badger/v3"
)

func (f *NodeState) Publish(req *pb.Publish, raftIndex uint64) (interface{}, error) {
	curTopic, err := f.getTopic(req.GetTopic())
	if err != nil {
		return nil, err
	}
	var res []*pb.Message
	newOffset := uint64(0)
	key := makeSeqKey(req.Topic, req.GetPartition())
	seq, err := f.MessageStore.GetSequence(key, 1000)
	defer seq.Release()
	err = f.MessageStore.Update(func(tx *badger.Txn) error {
		for _, m := range req.GetMessages() {
			offset, _ := seq.Next()
			offset++
			newOffset = offset
			newMsg := &pb.Message{
				Key:       m.GetKey(),
				Payload:   m.GetVal(),
				Topic:     req.GetTopic(),
				Partition: req.GetPartition(),
				Offset:    offset,
				RaftIndex: raftIndex,
			}
			byteProto, err := util.SerializeMessage(newMsg)
			if err != nil {
				f.Logger.Error().Msg("Error seralizing")
				f.Logger.Err(err)
				return err
			}
			msgKey := makeKey(req.Topic, req.Partition, offset)
			err = tx.Set(msgKey, byteProto)
			if err != nil {
				return err
			}
			res = append(res, newMsg)
		}
		return nil
	})
	if err != nil {
		f.Logger.Error().Msg("Error Writing to topic")
		f.Logger.Err(err)
		return nil, err
	}
	f.Logger.Debug().Msgf("Publish %v messages to partition %v topic %s", len(req.Messages), req.Partition, req.Topic)
	partition := curTopic.Partitions[req.Partition]
	partition.Offset = newOffset
	return res, nil
}
