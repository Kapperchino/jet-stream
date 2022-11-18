package main

import (
	"context"
	"github.com/Kapperchino/jet-leader-rpc/rafterrors"
	pb "github.com/Kapperchino/jet/proto"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"github.com/serialx/hashring"
	"github.com/spaolacci/murmur3"
	"github.com/tidwall/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type nodeState struct {
	topics   sync.Map
	messages boltdb.BoltStore
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

var _ raft.FSM = &nodeState{}

func (f *nodeState) Apply(l *raft.Log) interface{} {
	operation := &pb.Write{}
	err := proto.Unmarshal(l.Data, operation)
	if err != nil {
		log.Fatal("joe biden")
	}
	switch operation.Operation.(type) {
	case *pb.Write_Publish:
		return f.Publish(operation.GetPublish())
	case *pb.Write_CreateTopic:
		res := f.CreateTopic(operation.GetCreateTopic())
		return res
	case *pb.Write_CreateConsumer:
		break
	case *pb.Write_Consume:
		break
	}
	return nil
}

func (f *nodeState) Publish(req *pb.Publish) interface{} {
	item, _ := f.topics.Load(req.GetTopic())
	if item == nil {
		log.Printf("Topic %s does not exist", req.GetTopic())
		return nil
	}
	curTopic := item.(topic)
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
		curLog, err := wal.Open(curTopic.name+"/"+strconv.Itoa(i), nil)
		if err != nil {
			log.Fatal(err)
			return nil
		}
		curOffset := curTopic.partitions[i].offset.Load() + 1
		newOffset := curOffset
		batch := new(wal.Batch)
		for j, m := range bucket {
			byteProto, err := proto.Marshal(m)
			if err != nil {
				log.Fatal(err)
				return nil
			}
			newOffset = curOffset + uint64(j)
			batch.Write(newOffset, byteProto)
			res = append(res, &pb.Message{
				Key:       m.GetKey(),
				Payload:   m.GetVal(),
				Topic:     req.GetTopic(),
				Partition: int64(i),
				Offset:    int64(newOffset),
			})
		}
		curTopic.partitions[i].offset.Swap(newOffset)
		err = curLog.WriteBatch(batch)
		if err != nil {
			log.Fatal(err)
			return nil
		}
	}
	return res
}

func (f *nodeState) CreateTopic(req *pb.CreateTopic) interface{} {
	getTopic, _ := f.topics.Load(req.GetTopic())
	if getTopic != nil {
		return nil
	}
	err := os.Mkdir(req.GetTopic(), 0755)
	if err != nil {
		if !os.IsExist(err) {
			log.Fatal(err)
			return nil
		}
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
	f.topics.Store(req.GetTopic(), newTopic)
	return new(pb.CreateTopicResponse)
}

func (f *nodeState) CreatePartition(partitionNum int64, topic string) partition {
	res := partition{
		num:    partitionNum,
		topic:  topic,
		offset: new(atomic.Uint64),
	}
	return res
}

func (f *nodeState) Snapshot() (raft.FSMSnapshot, error) {
	//// Make sure that any future calls to f.Apply() don't change the snapshot.
	//return &snapshot{cloneWords(f.words)}, nil
	return nil, nil
}

func (f *nodeState) Restore(r io.ReadCloser) error {
	//b, err := io.ReadAll(r)
	//if err != nil {
	//	return err
	//}
	//words := strings.Split(string(b), "\n")
	//copy(f.words[:], words)
	return nil
}

func (s *nodeState) Persist(sink raft.SnapshotSink) error {
	//_, err := sink.Write([]byte(strings.Join(s.words, "\n")))
	//if err != nil {
	//	sink.Cancel()
	//	return fmt.Errorf("sink.Write(): %v", err)
	//}
	return sink.Close()
}

func (s *nodeState) Release() {
}

type rpcInterface struct {
	nodeState *nodeState
	raft      *raft.Raft
	pb.UnimplementedExampleServer
}

func PublishMessagesInternal(r rpcInterface, req *pb.PublishMessageRequest) ([]*pb.Message, error) {
	input := &pb.Write{
		Operation: &pb.Write_Publish{
			Publish: &pb.Publish{
				Topic:    req.GetTopic(),
				Messages: req.GetMessages(),
			},
		},
	}
	val, _ := proto.Marshal(input)
	res := r.raft.Apply(val, time.Second)
	if err := res.Error(); err != nil {
		return nil, rafterrors.MarkRetriable(err)
	}
	return res.Response().([]*pb.Message), nil
}

func (r rpcInterface) PublishMessages(ctx context.Context, req *pb.PublishMessageRequest) (*pb.PublishMessageResponse, error) {
	messages, err := PublishMessagesInternal(r, req)
	res := &pb.PublishMessageResponse{Messages: messages}
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (rpcInterface) CreateConsumer(ctx *pb.CreateConsumerRequest, server pb.Example_CreateConsumerServer) error {
	return status.Errorf(codes.Unimplemented, "method CreateConsumer not implemented")
}
func (rpcInterface) Consume(ctx context.Context, req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Consume not implemented")
}

func CreateTopicInternal(r rpcInterface, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	input := &pb.Write{
		Operation: &pb.Write_CreateTopic{
			CreateTopic: &pb.CreateTopic{
				Topic:      req.GetTopic(),
				Partitions: req.GetNumPartitions(),
			},
		},
	}
	val, _ := proto.Marshal(input)
	res := r.raft.Apply(val, time.Second)
	if err := res.Error(); err != nil {
		return nil, rafterrors.MarkRetriable(err)
	}
	return res.Response().(*pb.CreateTopicResponse), nil
}

func (r rpcInterface) CreateTopic(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
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
