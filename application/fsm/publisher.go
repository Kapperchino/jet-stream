package fsm

import (
	"encoding/binary"
	"github.com/Kapperchino/jet-application/util"
	pb "github.com/Kapperchino/jet/proto"
	"go.etcd.io/bbolt"
	"log"
)

func (f *NodeState) Publish(req *pb.Publish, raftIndex uint64) (interface{}, error) {
	curTopic, err := f.getTopic(req.GetTopic())
	if err != nil {
		return nil, err
	}
	buckets := make([][]*pb.KeyVal, len(curTopic.Partitions))
	var res []*pb.Message
	for _, m := range req.GetMessages() {
		curPartition, _ := curTopic.hashRing.GetNodePos(string(m.Key))
		buckets[curPartition] = append(buckets[curPartition], m)
	}
	for i, bucket := range buckets {
		if bucket == nil {
			continue
		}
		var lastRaftIndex uint64
		err := f.Topics.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(curTopic.Name))
			if b == nil {
				return nil
			}
			buffer := make([]byte, 8)
			binary.PutUvarint(buffer, uint64(i))
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
			log.Fatal(err)
			return nil, err
		}
		//no op if messages has already been written
		if lastRaftIndex >= raftIndex {
			return nil, nil
		}
		newOffset := uint64(0)
		for _, m := range bucket {
			err = f.Topics.Batch(func(tx *bbolt.Tx) error {
				b, _ := tx.CreateBucketIfNotExists([]byte(curTopic.Name))
				buffer := make([]byte, 8)
				binary.PutUvarint(buffer, uint64(i))
				b, _ = b.CreateBucketIfNotExists(buffer)
				offset, _ := b.NextSequence()
				newOffset = offset
				newMsg := &pb.Message{
					Key:       m.GetKey(),
					Payload:   m.GetVal(),
					Topic:     req.GetTopic(),
					Partition: int64(i),
					Offset:    int64(offset),
					RaftIndex: raftIndex,
				}
				byteProto, err := util.SerializeMessage(newMsg)
				if err != nil {
					log.Fatal(err)
					return err
				}
				buffer = make([]byte, 8)
				binary.PutUvarint(buffer, offset)
				err = b.Put(buffer, byteProto)
				if err != nil {
					return err
				}
				res = append(res, newMsg)
				return nil
			})
			if err != nil {
				log.Fatal(err)
				return nil, err
			}
		}
		curTopic.Partitions[i].Offset = newOffset
	}
	return res, nil
}
