package application

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/Kapperchino/jet-leader-rpc/rafterrors"
	pb "github.com/Kapperchino/jet/proto"
	"github.com/hashicorp/raft"
	"github.com/serialx/hashring"
	"github.com/spaolacci/murmur3"
	"go.etcd.io/bbolt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)

type NodeState struct {
	topics bbolt.DB
}

type topic struct {
	name       string
	partitions []partition
	hashRing   *hashring.HashRing
}

type partition struct {
	num    int64
	topic  string
	offset *atomic.Uint64
}

var _ raft.FSM = &NodeState{}

func (f *NodeState) Apply(l *raft.Log) interface{} {
	operation := &pb.Write{}
	err := deserializeMessage(l.Data, operation)
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
	buckets := make([][]*pb.KeyVal, len(curTopic.partitions))
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
		err := f.topics.View(func(tx *bbolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists([]byte(curTopic.name))
			buffer := make([]byte, 8)
			binary.PutUvarint(buffer, uint64(i))
			b, _ = b.CreateBucketIfNotExists(buffer)
			_, val := b.Cursor().Last()
			var lastMsg pb.Message
			err := deserializeMessage(val, &lastMsg)
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
		curOffSet := curTopic.partitions[i].offset.Load()
		newOffset := curOffSet + 1
		for j, m := range bucket {
			newOffset += uint64(j)
			newMsg := &pb.Message{
				Key:       m.GetKey(),
				Payload:   m.GetVal(),
				Topic:     req.GetTopic(),
				Partition: int64(i),
				Offset:    int64(newOffset),
				RaftIndex: raftIndex,
			}
			byteProto, err := serializeMessage(newMsg)
			if err != nil {
				log.Fatal(err)
				return nil, err
			}
			err = f.topics.Batch(func(tx *bbolt.Tx) error {
				b, _ := tx.CreateBucketIfNotExists([]byte(curTopic.name))
				buffer := make([]byte, 8)
				binary.PutUvarint(buffer, uint64(i))
				b, _ = b.CreateBucketIfNotExists(buffer)
				err = b.Put(newMsg.GetKey(), byteProto)
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				log.Fatal(err)
				return nil, err
			}
			res = append(res, newMsg)
		}
		curTopic.partitions[i].offset.Swap(newOffset + 1)
	}
	return res, nil
}

func (f *NodeState) CreateTopic(req *pb.CreateTopic) (interface{}, error) {
	curTopic, err := f.getTopic(req.GetTopic())
	//already exists
	if err != nil && curTopic != nil {
		return new(pb.CreateTopicResponse), nil
	}
	newTopic := topic{
		name:       req.GetTopic(),
		partitions: []partition{},
		hashRing:   nil,
	}
	hasher := murmur3.New64()
	var partitionHashed []string
	for i := int64(0); i < req.GetPartitions(); i++ {
		newTopic.partitions = append(newTopic.partitions, f.CreatePartition(i, req.GetTopic()))
		_, _ = hasher.Write(make([]byte, i))
		partitionHashed = append(partitionHashed, strconv.FormatUint(hasher.Sum64(), 2))
		hasher.Reset()
	}
	newTopic.hashRing = hashring.New(partitionHashed)
	//seralize topic and put in db
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(newTopic)
	if err != nil {
		return nil, fmt.Errorf("error encoding topic")
	}
	err = f.topics.Update(func(tx *bbolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("Topics"))
		err = b.Put([]byte(newTopic.name), buf.Bytes())
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error saving topic")
	}
	return new(pb.CreateTopicResponse), nil
}

func (f *NodeState) getTopic(topicName string) (*topic, error) {
	var curTopic *topic
	err := f.topics.View(func(tx *bbolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("Topics"))
		v := b.Get([]byte(topicName))
		buf := bytes.NewBuffer(v)
		dec := gob.NewDecoder(buf)
		if err := dec.Decode(&curTopic); err != nil {
			log.Fatal(err)
			return err
		}
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

func (f *NodeState) CreatePartition(partitionNum int64, topic string) partition {
	res := partition{
		num:    partitionNum,
		topic:  topic,
		offset: new(atomic.Uint64),
	}
	res.offset.Store(1)
	return res
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

type RpcInterface struct {
	NodeState *NodeState
	Raft      *raft.Raft
	pb.UnimplementedExampleServer
}

func PublishMessagesInternal(r RpcInterface, req *pb.PublishMessageRequest) ([]*pb.Message, error) {
	input := &pb.Write{
		Operation: &pb.Write_Publish{
			Publish: &pb.Publish{
				Topic:    req.GetTopic(),
				Messages: req.GetMessages(),
			},
		},
	}
	val, _ := serializeMessage(input)
	res := r.Raft.Apply(val, time.Second)
	if err := res.Error(); err != nil {
		return nil, rafterrors.MarkRetriable(err)
	}
	return res.Response().([]*pb.Message), nil
}

func (r RpcInterface) PublishMessages(ctx context.Context, req *pb.PublishMessageRequest) (*pb.PublishMessageResponse, error) {
	messages, err := PublishMessagesInternal(r, req)
	res := &pb.PublishMessageResponse{Messages: messages}
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (RpcInterface) CreateConsumer(ctx *pb.CreateConsumerRequest, server pb.Example_CreateConsumerServer) error {
	return status.Errorf(codes.Unimplemented, "method CreateConsumer not implemented")
}
func (RpcInterface) Consume(ctx context.Context, req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Consume not implemented")
}

func CreateTopicInternal(r RpcInterface, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	input := &pb.Write{
		Operation: &pb.Write_CreateTopic{
			CreateTopic: &pb.CreateTopic{
				Topic:      req.GetTopic(),
				Partitions: req.GetNumPartitions(),
			},
		},
	}
	val, _ := serializeMessage(input)
	res := r.Raft.Apply(val, time.Second)
	if err := res.Error(); err != nil {
		return nil, rafterrors.MarkRetriable(err)
	}
	return res.Response().(*pb.CreateTopicResponse), nil
}

func (r RpcInterface) CreateTopic(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	res, err := CreateTopicInternal(r, req)
	if err != nil {
		return nil, err
	}
	log.Printf("Created topic %s", req.GetTopic())
	return res, nil
}

//func (r rpcInterface) AddWord(ctx context.Context, req *pb.AddWordRequest) (*pb.AddWordResponse, error) {
//	f := r.raft.Apply([]byte(req.GetWord()), time.Second)
//	if err := f.Error(); err != nil {
//		return nil, rafterrors.MarkRetriable(err)
//	}
//	return &pb.AddWordResponse{
//		CommitIndex: f.Index(),
//	}, nil
//}
