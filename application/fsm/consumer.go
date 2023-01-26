package fsm

import (
	"bytes"
	"encoding/gob"
	"fmt"
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/Kapperchino/jet/util"
	"github.com/dgraph-io/badger/v3"
	"github.com/rs/zerolog/log"
	"strconv"
)

type Consumer struct {
	Id          uint64
	Checkpoints []Checkpoint
}

type Checkpoint struct {
	Offset    uint64
	Partition uint64
}

func (f *NodeState) CreateConsumer(req *pb.CreateConsumer) (interface{}, error) {
	topic, err := f.getTopic(req.GetTopic())
	if err != nil {
		return nil, err
	}
	response := new(pb.CreateConsumerResponse)
	seq, err := f.MessageStore.GetSequence([]byte("Consumer-"), 1000)
	defer seq.Release()
	err = f.MetaStore.Update(func(tx *badger.Txn) error {
		id, _ := seq.Next()
		list := make([]Checkpoint, len(topic.Partitions))
		for i := 0; i < len(topic.Partitions); i++ {
			list = append(list, Checkpoint{
				Offset:    1,
				Partition: uint64(i),
			})
		}
		consumer := Consumer{
			Id:          id,
			Checkpoints: list,
		}
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(consumer)
		if err != nil {
			log.Printf("error encoding Topic, %s", err)
			return fmt.Errorf("error encoding Topic, %w", err)
		}
		consumerId := []byte("Consumer-" + strconv.FormatUint(id, 10))
		err = tx.Set(consumerId, buf.Bytes())
		if err != nil {
			log.Printf("error putting items in bucket, %s", err)
			return fmt.Errorf("error putting items in bucket, %w", err)
		}
		response.ConsumerId = int64(id)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error with local store %w", err)
	}
	return response, nil
}

func (f *NodeState) Consume(req *pb.Consume) (interface{}, error) {
	topic, err := f.getTopic(req.GetTopic())
	if err != nil {
		return nil, fmt.Errorf("topic does not exist, %w", err)
	}
	res := pb.ConsumeResponse{
		Messages:  make([]*pb.Message, 0),
		LastIndex: 0,
	}
	err = f.MetaStore.View(func(tx *badger.Txn) error {
		consumer, err := f.getConsumer(uint64(req.GetId()))
		if err != nil {
			log.Err(err).Msgf("Error getting consumer")
			return err
		}
		for i, _ := range topic.Partitions {
			curCheckpoint := consumer.Checkpoints[i]
			var buf []*pb.Message
			startingOffset := curCheckpoint.Offset
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 100
			it := tx.NewIterator(opts)
			defer it.Close()
			key := topic.Name + "-" + strconv.FormatInt(int64(i), 10)
			it.Seek([]byte(key + "-" + strconv.FormatUint(startingOffset, 10)))
			for it.ValidForPrefix([]byte(key)); it.Valid(); it.Next() {
				//now we need to seek until the message is found
				item := it.Item()
				err := item.Value(func(v []byte) error {
					var message pb.Message
					err := util.DeserializeMessage(v, &message)
					if err != nil {
						return err
					}
					buf = append(buf, &message)
					return nil
				})
				if err != nil {
					return err
				}
			}
			res.Messages = append(res.Messages, buf...)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error with local store %w", err)
	}
	return &res, nil
}

func (f *NodeState) getConsumer(consumerId uint64) (*Consumer, error) {
	var consumer *Consumer
	err := f.MetaStore.View(func(tx *badger.Txn) error {
		v, err := tx.Get([]byte("Consumer-" + strconv.FormatUint(consumerId, 10)))
		if err != nil {
			return nil
		}
		var vbytes []byte
		v.Value(func(val []byte) error {
			vbytes = val
			return nil
		})
		buf := bytes.NewBuffer(vbytes)
		dec := gob.NewDecoder(buf)
		if err := dec.Decode(&consumer); err != nil {
			return fmt.Errorf("decoding issues with this %w", err)
		}
		return nil
	})
	if consumer == nil {
		return nil, fmt.Errorf("consumer does not exist")
	} else if err != nil {
		return nil, fmt.Errorf("error with local store, %s", err)
	}
	return consumer, nil
}
