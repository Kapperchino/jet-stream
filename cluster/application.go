package cluster

import (
	"context"
	"github.com/Kapperchino/jet-application/fsm"
	pb "github.com/Kapperchino/jet-cluster/proto"
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
	ShardId  string
	isLeader bool
}

var raftChan chan raft.Observation

func init() {
	raftChan = make(chan raft.Observation, 100)
	raft.NewObserver(raftChan, false, nil)
	go onRaftUpdates(raftChan)
}

func onRaftUpdates(raftChan chan raft.Observation) {
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
