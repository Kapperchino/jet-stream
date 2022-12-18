package fsm

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/Kapperchino/jet-application/util"
	pb "github.com/Kapperchino/jet/proto"
	"github.com/hashicorp/raft"
	"github.com/serialx/hashring"
	"github.com/spaolacci/murmur3"
	"go.etcd.io/bbolt"
	"io"
	"log"
	"strconv"
)

type Topic struct {
	Name       string
	Partitions []Partition
	hashRing   *hashring.HashRing
}

type Partition struct {
	Num    int64
	Topic  string
	Offset uint64
}

type Consumer struct {
	Id          uint64
	Checkpoints []Checkpoint
}

type Checkpoint struct {
	Offset    uint64
	Partition uint64
}

type NodeState struct {
	Topics *bbolt.DB
}

var _ raft.FSM = &NodeState{}

func (f *NodeState) Apply(l *raft.Log) interface{} {
	operation := &pb.Write{}
	err := util.DeserializeMessage(l.Data, operation)
	if err != nil {
		log.Fatal(err)
	}
	switch operation.Operation.(type) {
	case *pb.Write_Publish:
		res, _ := f.Publish(operation.GetPublish(), l.Index)
		return res
	case *pb.Write_CreateTopic:
		res, _ := f.CreateTopic(operation.GetCreateTopic())
		return res
	case *pb.Write_CreateConsumer:
		break
	case *pb.Write_Consume:
		break
	}
	return nil
}

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
				binary.PutUvarint(buffer, uint64(offset))
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

func (f *NodeState) CreateTopic(req *pb.CreateTopic) (interface{}, error) {
	curTopic, err := f.getTopic(req.GetTopic())
	//already exists
	if err == nil && curTopic != nil {
		return new(pb.CreateTopicResponse), nil
	}
	newTopic := Topic{
		Name:       req.GetTopic(),
		Partitions: []Partition{},
		hashRing:   nil,
	}
	hasher := murmur3.New64()
	var partitionHashed []string
	for i := int64(0); i < req.GetPartitions(); i++ {
		newTopic.Partitions = append(newTopic.Partitions, f.CreatePartition(i, req.GetTopic()))
		_, _ = hasher.Write(make([]byte, i))
		partitionHashed = append(partitionHashed, strconv.FormatUint(hasher.Sum64(), 2))
		hasher.Reset()
	}
	newTopic.hashRing = hashring.New(partitionHashed)
	//seralize Topic and put in db
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(newTopic)
	if err != nil {
		return nil, fmt.Errorf("error encoding Topic")
	}
	err = f.Topics.Update(func(tx *bbolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("Topics"))
		err = b.Put([]byte(newTopic.Name), buf.Bytes())
		if err != nil {
			return err
		}
		log.Printf("Created Topic %s", req.GetTopic())
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error saving Topic")
	}
	return new(pb.CreateTopicResponse), nil
}

func (f *NodeState) getTopic(topicName string) (*Topic, error) {
	var curTopic *Topic
	err := f.Topics.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("Topics"))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(topicName))
		buf := bytes.NewBuffer(v)
		dec := gob.NewDecoder(buf)
		if err := dec.Decode(&curTopic); err != nil {
			log.Fatal(err)
			return err
		}
		hasher := murmur3.New64()
		var partitionHashed []string
		for i := 0; i < len(curTopic.Partitions); i++ {
			_, _ = hasher.Write(make([]byte, i))
			partitionHashed = append(partitionHashed, strconv.FormatUint(hasher.Sum64(), 2))
			hasher.Reset()
		}
		curTopic.hashRing = hashring.New(partitionHashed)
		return nil
	})
	if curTopic == nil {
		log.Printf("Topic %s does not exist", topicName)
		return nil, err
	} else if err != nil {
		return nil, fmt.Errorf("error with local store, %s", err)
	}
	return curTopic, nil
}

func (f *NodeState) CreatePartition(partitionNum int64, topic string) Partition {
	res := Partition{
		Num:    partitionNum,
		Topic:  topic,
		Offset: 1,
	}
	return res
}

func (f *NodeState) CreateConsumer(req *pb.CreateConsumer) (interface{}, error) {
	topic, err := f.getTopic(req.GetTopic())
	id := uint64(0)
	if err != nil {
		return nil, err
	}
	err = f.Topics.Batch(func(tx *bbolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("Consumers"))
		id1, _ := b.NextSequence()
		id = id1
		buffer := make([]byte, 8)
		binary.PutUvarint(buffer, id)
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
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error with local store %w", err)
	}
	return pb.CreateConsumerResult{}, nil
}

func (f *NodeState) Snapshot() (raft.FSMSnapshot, error) {
	//// Make sure that any future calls to f.Apply() don't change the snapshot.
	//return &snapshot{cloneWords(f.words)}, nil
	return nil, nil
}

func (f *NodeState) Restore(r io.ReadCloser) error {
	//b, err := io.ReadAll(r)
	//if err != nil {
	//	return err
	//}
	//words := strings.Split(string(b), "\n")
	//copy(f.words[:], words)
	return nil
}

func (s *NodeState) Persist(sink raft.SnapshotSink) error {
	//_, err := sink.Write([]byte(strings.Join(s.words, "\n")))
	//if err != nil {
	//	sink.Cancel()
	//	return fmt.Errorf("sink.Write(): %v", err)
	//}
	return sink.Close()
}

func (s *NodeState) Release() {
}
