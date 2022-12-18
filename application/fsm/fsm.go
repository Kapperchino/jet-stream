package fsm

import (
	"github.com/Kapperchino/jet-application/util"
	pb "github.com/Kapperchino/jet/proto"
	"github.com/hashicorp/raft"
	"go.etcd.io/bbolt"
	"io"
	"log"
)

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
