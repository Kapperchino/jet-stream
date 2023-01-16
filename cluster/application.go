package cluster

import (
	"context"
	pb "github.com/Kapperchino/jet-cluster/proto"
	"github.com/alphadose/haxmap"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"
	"time"
)

type RpcInterface struct {
	Raft         *raft.Raft
	ClusterState *ClusterState
	pb.UnimplementedClusterMetaServiceServer
}

type ClusterState struct {
	shardId  string
	isLeader bool
	shardMap haxmap.Map[string, ShardInfo]
	raftChan chan raft.Observation
	Members  *memberlist.Memberlist
}

type ShardInfo struct {
	shardId  string
	isLeader bool
}

func (i *RpcInterface) InitClusterState(memberlist *memberlist.Memberlist) {
	i.ClusterState = &ClusterState{
		isLeader: false,
		shardMap: haxmap.Map[string, ShardInfo]{},
		raftChan: make(chan raft.Observation, 100),
		Members:  memberlist,
	}
	raft.NewObserver(i.ClusterState.raftChan, false, nil)
	go onRaftUpdates(i.ClusterState.raftChan)
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
	members := r.ClusterState.Members.Members()
	arr := make([]string, len(members))
	for x := 0; x < len(arr); x++ {
		arr[x] = members[x].Name
	}
	return &pb.GetPeersResponse{Peers: arr}, nil
}
