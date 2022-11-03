package main

import (
	"context"
	pb "github.com/Kapperchino/jet/proto"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"sync"
	"sync/atomic"
)

type nodeState struct {
	topics sync.Map
}

type topic struct {
	name       string
	partitions []partition
}

type partition struct {
	num      int64
	topic    string
	offset   atomic.Uint64
	messages []int64
}

var _ raft.FSM = &nodeState{}

func (f *nodeState) Apply(l *raft.Log) interface{} {
	//w := string(l.Data)
	//for i := 0; i < len(f.words); i++ {
	//	if compareWords(w, f.words[i]) {
	//		copy(f.words[i+1:], f.words[i:])
	//		f.words[i] = w
	//		break
	//	}
	//}
	return nil
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

type snapshot struct {
	words []string
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

func (rpcInterface) PublishMessages(ctx context.Context, req *pb.PublishMessageRequest) (*pb.PublishMessageResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PublishMessages not implemented")
}
func (rpcInterface) CreateConsumer(ctx *pb.CreateConsumerRequest, server pb.Example_CreateConsumerServer) error {
	return status.Errorf(codes.Unimplemented, "method CreateConsumer not implemented")
}
func (rpcInterface) Consume(ctx context.Context, req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Consume not implemented")
}
func (rpcInterface) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateTopic not implemented")
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
