package fsm

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/Kapperchino/jet-application/config"
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/Kapperchino/jet-application/util"
	"go.etcd.io/bbolt"
	"log"
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
	err = f.Topics.Update(func(tx *bbolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("Consumers"))
		id, _ := b.NextSequence()
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
		err = b.Put(util.ULongToBytes(id), buf.Bytes())
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
	err = f.Topics.View(func(tx *bbolt.Tx) error {
		consumers := tx.Bucket([]byte("Consumers"))
		if consumers == nil {
			return fmt.Errorf("bucket does not exist")
		}
		consumerBytes := consumers.Get(util.ULongToBytes(uint64(req.GetId())))
		if consumerBytes == nil {
			return fmt.Errorf("consumer with id %d does not exist", req.GetId())
		}
		var consumer *Consumer
		buf := bytes.NewBuffer(consumerBytes)
		dec := gob.NewDecoder(buf)
		if err := dec.Decode(&consumer); err != nil {
			return fmt.Errorf("decoding issues with this %w", err)
		}
		baseBucket := tx.Bucket([]byte("Topics"))
		if baseBucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		topicBucket := baseBucket.Bucket([]byte(req.GetTopic()))
		if topicBucket == nil {
			return fmt.Errorf("bucket does not exit")
		}
		for i, _ := range topic.Partitions {
			curCheckpoint := consumer.Checkpoints[i]
			partitionBucket := topicBucket.Bucket(util.ULongToBytes(uint64(i)))
			if partitionBucket == nil {
				continue
			}
			cursor := partitionBucket.Cursor()
			if err != nil {
				return fmt.Errorf("Error getting cursor %w", err)
			}
			var buf []*pb.Message
			startingOffset := curCheckpoint.Offset
			for k, v := cursor.Seek(util.ULongToBytes(curCheckpoint.Offset)); k != nil && util.BytesToULong(k) < startingOffset+config.CONSUME_CHUNK; k, v = cursor.Next() {
				msg := &pb.Message{}
				err := util.DeserializeMessage(v, msg)
				if err != nil {
					return fmt.Errorf("error deseralize %w", err)
				}
				curCheckpoint.Offset++
				buf = append(buf, msg)
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
	err := f.Topics.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("Consumers"))
		if b == nil {
			return nil
		}
		v := b.Get(util.ULongToBytes(consumerId))
		if v == nil {
			return nil
		}
		buf := bytes.NewBuffer(v)
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
