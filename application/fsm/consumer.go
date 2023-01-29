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

// CreateConsumer write operation, done in fsm
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
		response.ConsumerId = id
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error with local store %w", err)
	}
	return response, nil
}

// Consume operation, should be done in replicas and not in fsm
func (f *NodeState) Consume(req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	topic, err := f.getTopic(req.GetTopic())
	if err != nil {
		return nil, fmt.Errorf("topic does not exist, %w", err)
	}
	res := pb.ConsumeResponse{
		Messages:  make([]*pb.Message, 0),
		LastIndex: 0,
	}
	err = f.MetaStore.View(func(tx *badger.Txn) error {
		consumer, err := f.getConsumer(req.GetConsumerId())
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
			key := makePrefix(req.Topic, uint64(i))
			it.Seek(makeKey(req.Topic, uint64(i), startingOffset+1))
			for it.ValidForPrefix(key); it.Valid(); it.Next() {
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

func (f *NodeState) Ack(request *pb.Ack) (interface{}, error) {
	consumer, err := f.getConsumer(request.Id)
	if err != nil {
		return nil, err
	}
	for partition, offset := range request.Offsets {
		consumer.Checkpoints[partition].Offset = offset
	}

	err = f.MetaStore.Update(func(tx *badger.Txn) error {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(consumer)
		if err != nil {
			log.Printf("error encoding Topic, %s", err)
			return fmt.Errorf("error encoding Topic, %w", err)
		}
		consumerId := []byte("Consumer-" + strconv.FormatUint(consumer.Id, 10))
		err = tx.Set(consumerId, buf.Bytes())
		if err != nil {
			log.Printf("error putting items in bucket, %s", err)
			return fmt.Errorf("error putting items in bucket, %w", err)
		}
		return nil
	})
	return &pb.AckConsumeResponse{}, nil
}
