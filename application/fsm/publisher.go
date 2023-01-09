package fsm

import (
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/Kapperchino/jet-application/util"
	"github.com/rs/zerolog/log"
	"go.etcd.io/bbolt"
)

func (f *NodeState) Publish(req *pb.Publish, raftIndex uint64) (interface{}, error) {
	curTopic, err := f.getTopic(req.GetTopic())
	if err != nil {
		return nil, err
	}
	var res []*pb.Message
	var lastRaftIndex uint64
	err = f.Topics.View(func(tx *bbolt.Tx) error {
		baseBucket := tx.Bucket([]byte("Topics"))
		if baseBucket == nil {
			return nil
		}
		b := baseBucket.Bucket([]byte(curTopic.Name))
		if b == nil {
			return nil
		}
		buffer := util.ULongToBytes(uint64(req.GetPartition()))
		b = b.Bucket(buffer)
		if b == nil {
			return nil
		}
		_, val := b.Cursor().Last()
		var lastMsg pb.Message
		err := util.DeserializeMessage(val, &lastMsg)
		if err != nil {
			return err
		}
		lastRaftIndex = lastMsg.RaftIndex
		return nil
	})
	if err != nil {
		log.Error().Msg("Cannot get the latest raftIndex")
		log.Err(err)
		return nil, err
	}
	//no op if messages has already been written
	if lastRaftIndex >= raftIndex {
		return nil, nil
	}
	newOffset := uint64(0)
	err = f.Topics.Batch(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("Topics"))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Error().Msg("Cannot create topic")
		log.Err(err)
		return nil, err
	}
	for _, m := range req.GetMessages() {
		err = f.Topics.Batch(func(tx *bbolt.Tx) error {
			baseBucket := tx.Bucket([]byte("Topics"))
			b, _ := baseBucket.CreateBucketIfNotExists([]byte(curTopic.Name))
			b, _ = b.CreateBucketIfNotExists(util.ULongToBytes(uint64(req.GetPartition())))
			offset, _ := b.NextSequence()
			newOffset = offset
			newMsg := &pb.Message{
				Key:       m.GetKey(),
				Payload:   m.GetVal(),
				Topic:     req.GetTopic(),
				Partition: req.GetPartition(),
				Offset:    int64(offset),
				RaftIndex: raftIndex,
			}
			byteProto, err := util.SerializeMessage(newMsg)
			if err != nil {
				log.Error().Msg("Error seralizing")
				log.Err(err)
				return err
			}
			err = b.Put(util.ULongToBytes(offset), byteProto)
			if err != nil {
				return err
			}
			res = append(res, newMsg)
			return nil
		})
		if err != nil {
			log.Error().Msg("Error Writing to topic")
			log.Err(err)
			return nil, err
		}
	}
	curTopic.Partitions[req.GetPartition()].Offset = newOffset
	return res, nil
}
