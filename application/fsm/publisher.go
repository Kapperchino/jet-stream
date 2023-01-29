package fsm

import (
	"fmt"
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/Kapperchino/jet/util"
	"github.com/dgraph-io/badger/v3"
	"github.com/rs/zerolog/log"
	"strconv"
)

func (f *NodeState) Publish(req *pb.Publish, raftIndex uint64) (interface{}, error) {
	curTopic, err := f.getTopic(req.GetTopic())
	if err != nil {
		return nil, err
	}
	var res []*pb.Message
	lastRaftIndex, err := f.getLastIndex(req.GetTopic(), int64(req.GetPartition()))
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
	key := makePrefix(req.Topic, req.Partition)
	seq, err := f.MessageStore.GetSequence(key, 1000)
	defer seq.Release()
	for _, m := range req.GetMessages() {
		err = f.MetaStore.Update(func(tx *badger.Txn) error {
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
				log.Error().Msg("Error seralizing")
				log.Err(err)
				return err
			}
			msgKey := makeKey(req.Topic, req.Partition, offset)
			err = tx.Set(msgKey, byteProto)
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

func (f *NodeState) getLastIndex(topic string, partition int64) (uint64, error) {
	var lastRaftIndex uint64
	err := f.MetaStore.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		opts.PrefetchSize = 100
		it := tx.NewIterator(opts)
		defer it.Close()
		key := topic + "-" + strconv.FormatInt(partition, 10)
		for it.ValidForPrefix([]byte(key)); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				fmt.Printf("key=%s, value=%s\n", k, v)
				var message pb.Message
				err := util.DeserializeMessage(v, &message)
				if err != nil {
					return err
				}
				lastRaftIndex = message.RaftIndex
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return lastRaftIndex, err
	}
	return lastRaftIndex, nil
}
