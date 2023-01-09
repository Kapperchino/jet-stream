package fsm

import (
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/Kapperchino/jet-application/util"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"
	"go.etcd.io/bbolt"
	"io"
)

type NodeState struct {
	Topics  *bbolt.DB
	Members *memberlist.Memberlist
}

var _ raft.FSM = &NodeState{}

func (f *NodeState) Apply(l *raft.Log) interface{} {
	operation := &pb.Write{}
	err := util.DeserializeMessage(l.Data, operation)
	if err != nil {
		log.Error().Err(err)
		return err
	}
	switch operation.Operation.(type) {
	case *pb.Write_Publish:
		res, err := f.Publish(operation.GetPublish(), l.Index)
		if err != nil {
			log.Error().Err(err)
			return err
		}
		return res
	case *pb.Write_CreateTopic:
		res, err := f.CreateTopic(operation.GetCreateTopic())
		if err != nil {
			log.Error().Err(err)
			return err
		}
		return res
	case *pb.Write_CreateConsumer:
		res, err := f.CreateConsumer(operation.GetCreateConsumer())
		if err != nil {
			log.Error().Err(err)
			return err
		}
		return res
	case *pb.Write_Consume:
		res, err := f.Consume(operation.GetConsume())
		if err != nil {
			log.Error().Err(err)
			return err
		}
		return res
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
