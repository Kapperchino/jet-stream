package fsm

import (
	pb "github.com/Kapperchino/jet-stream/application/proto/proto"
	cluster "github.com/Kapperchino/jet-stream/cluster"
	"github.com/Kapperchino/jet-stream/util"
	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
	"io"
)

type NodeState struct {
	MetaStore    *badger.DB
	MessageStore *badger.DB
	HandlerMap   []func(f *NodeState, op *pb.WriteOperation, l *raft.Log) interface{}
	ShardState   *cluster.ShardState
	Logger       *zerolog.Logger
}

var _ raft.FSM = &NodeState{}

func (f *NodeState) Apply(l *raft.Log) interface{} {
	operation := &pb.WriteOperation{}
	err := util.DeserializeMessage(l.Data, operation)
	if err != nil {
		f.Logger.Error().Err(err)
		return err
	}
	return f.HandlerMap[operation.Code](f, operation, l)
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

func (f *NodeState) Persist(sink raft.SnapshotSink) error {
	//_, err := sink.Write([]byte(strings.Join(s.words, "\n")))
	//if err != nil {
	//	sink.Cancel()
	//	return fmt.Errorf("sink.Write(): %v", err)
	//}
	return sink.Close()
}

func (f *NodeState) Release() {
}
