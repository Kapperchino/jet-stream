package cluster

import (
	"context"
	"github.com/Kapperchino/jet-application/fsm"
	pb "github.com/Kapperchino/jet-cluster/proto"
	"github.com/alphadose/haxmap"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"
	"time"
)

type RpcInterface struct {
	NodeState *fsm.NodeState
	Raft      *raft.Raft
	pb.UnimplementedClusterMetaServiceServer
}

type ShardState struct {
	shardId   string
	isLeader  bool
	memberMap haxmap.Map[string, string]
}

var raftChan chan raft.Observation

func (s *ShardState) init() {
	raftChan = make(chan raft.Observation, 100)
	s.memberMap = haxmap.Map[string, string]{}
	raft.NewObserver(raftChan, false, nil)
	go s.onRaftUpdates(raftChan)
}

func (s *ShardState) onRaftUpdates(raftChan chan raft.Observation) {
	for {
		time.Sleep(time.Second * 3)
		select {
		case observable := <-raftChan:
			switch val := observable.Data.(type) {
			case raft.RequestVoteRequest:
				log.Info().Msg("vote requested")
				break
			case raft.RaftState:
				state := val.String()
				if state == "Leader" {
					log.Info().Msg("State is now leader")
				}
				break
			case raft.PeerObservation:
				log.Info().Msg("peer observation")
				break
			case raft.LeaderObservation:
				log.Info().Msg("leader changed")
				break
			}
			log.Debug().Msgf("received message %s", observable)
		default:
			log.Debug().Msgf("No updates for shard")
		}
	}

}

func (r RpcInterface) GetPeers(_ context.Context, req *pb.GetPeersRequest) (*pb.GetPeersResponse, error) {
	members := r.NodeState.Members.Members()
	arr := make([]string, len(members))
	for x := 0; x < len(arr); x++ {
		arr[x] = members[x].Name
	}
	return &pb.GetPeersResponse{Peers: arr}, nil
}
