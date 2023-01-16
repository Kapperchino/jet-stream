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
	ShardId  string
	isLeader bool
	shardMap haxmap.Map[string, ShardInfo]
	raftChan chan raft.Observation
	Members  *memberlist.Memberlist
}

type ShardInfo struct {
	shardId  string
	isLeader bool
}

func InitClusterState(memberlist *memberlist.Memberlist, i *RpcInterface) {
	i.ClusterState = &ClusterState{
		isLeader: false,
		shardMap: haxmap.Map[string, ShardInfo]{},
		raftChan: make(chan raft.Observation, 3),
		Members:  memberlist,
	}
	observer := raft.NewObserver(i.ClusterState.raftChan, false, nil)
	i.Raft.RegisterObserver(observer)
	go onRaftUpdates(i.ClusterState.raftChan, i)
}

func onRaftUpdates(raftChan chan raft.Observation, i *RpcInterface) {
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
				log.Info().Msgf("State is now %s", state)
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
			log.Debug().Msgf("No updates for shard %s", i.ClusterState.ShardId)
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
