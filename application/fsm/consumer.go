package fsm

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/Kapperchino/jet-application/config"
	"github.com/Kapperchino/jet-application/util"
	pb "github.com/Kapperchino/jet/proto"
	"go.etcd.io/bbolt"
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
	err = f.Topics.Batch(func(tx *bbolt.Tx) error {
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
			return fmt.Errorf("error encoding Topic, %w", err)
		}
		err = b.Put(util.ULongToBytes(id), buf.Bytes())
		if err != nil {
			return fmt.Errorf("error putting items in bucket, %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error with local store %w", err)
	}
	return pb.CreateConsumerResult{}, nil
}

func (f *NodeState) Consume(req *pb.Consume) (interface{}, error) {
	topic, err := f.getTopic(req.GetTopic())
	if err != nil {
		return nil, fmt.Errorf("topic does not exist, %w", err)
	}
	err = f.Topics.View(func(tx *bbolt.Tx) error {
		consumers := tx.Bucket([]byte("Consumers"))
		if consumers == nil {
			return fmt.Errorf("bucket does not exist")
		}
		consumerBytes := consumers.Get(util.LongToBytes(req.GetId()))
		if consumerBytes == nil {
			return fmt.Errorf("consumer does not exist")
		}
		var consumer *Consumer
		buf := bytes.NewBuffer(consumerBytes)
		dec := gob.NewDecoder(buf)
		if err := dec.Decode(&consumer); err != nil {
			return fmt.Errorf("decoding issues with this %w", err)
		}
		//TODO: loop through partitions of the topic, start at the offset at the checkpoint, then consume from partition and add to checkpoint
		topicBucket := tx.Bucket([]byte("Topics"))
		if topicBucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		for i, _ := range topic.Partitions {
			curCheckpoint := consumer.Checkpoints[i]
			partitionBucket := topicBucket.Bucket(util.LongToBytes(int64(i)))
			cursor := partitionBucket.Cursor()
			if err != nil {
				return fmt.Errorf("Error getting cursor")
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
				fmt.Printf("key=%s, value=%s\n", k, v)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error with local store %w", err)
	}
	return pb.CreateConsumerResult{}, nil
}
