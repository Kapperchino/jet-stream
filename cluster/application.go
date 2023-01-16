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
	NodeId   string
	isLeader bool
	Leader   string
	shardMap *haxmap.Map[string, ShardInfo]
	raftChan chan raft.Observation
	Members  *memberlist.Memberlist
}

type ShardInfo struct {
	shardId  string
	isLeader bool
	address  string
}

func InitClusterState(memberlist *memberlist.Memberlist, i *RpcInterface) {
	i.ClusterState = &ClusterState{
		isLeader: false,
		shardMap: haxmap.New[string, ShardInfo](),
		raftChan: make(chan raft.Observation, 50),
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
				log.Debug().Msg("vote requested")
				break
			case raft.RaftState:
				onStateUpdate(i, val)
				break
			case raft.PeerObservation:
				onPeerUpdate(i, val)
				break
			case raft.LeaderObservation:
				onLeaderUpdate(i, val)
				break
			}
		default:
			log.Debug().Msgf("No updates for shard %s", i.ClusterState.ShardId)
		}
	}

}

func onPeerUpdate(i *RpcInterface, update raft.PeerObservation) {
	if update.Removed {
		log.Info().Msgf("Peer %s is removed", update.Peer.ID)
		i.ClusterState.shardMap.Del(string(update.Peer.ID))
		return
	}
	//peer is updated
	_, exist := i.ClusterState.shardMap.Get(string(update.Peer.ID))
	if exist {
		log.Debug().Msgf("Peer %s already exists, no-op", update.Peer.ID)
	}
	//add peer
	i.ClusterState.shardMap.Set(string(update.Peer.ID), ShardInfo{
		shardId:  string(update.Peer.ID),
		isLeader: false,
		address:  string(update.Peer.Address),
	})
	log.Info().Msgf("Peer %s is added", update.Peer.ID)
}

func onLeaderUpdate(i *RpcInterface, update raft.LeaderObservation) {
	if string(update.LeaderID) == i.ClusterState.NodeId {
		log.Debug().Msg("Node is already leader, no-op")
		return
	}
	//leadership update
	i.ClusterState.isLeader = false
	i.ClusterState.Leader = string(update.LeaderID)
	_, exist := i.ClusterState.shardMap.Get(string(update.LeaderID))
	if exist {
		log.Debug().Msgf("Leader %s already exists, no-op", update.LeaderAddr)
		return
	}
	i.ClusterState.shardMap.Set(string(update.LeaderID), ShardInfo{
		shardId:  string(update.LeaderID),
		isLeader: true,
		address:  string(update.LeaderAddr),
	})
}

func onStateUpdate(i *RpcInterface, state raft.RaftState) {
	switch state {
	case raft.Follower:
		onFollowingState(i)
		break
	case raft.Leader:
		onGainingLeaderShip(i)
		break
	case raft.Shutdown:
		log.Warn().Msg("Shutting down")
		break
	case raft.Candidate:
		log.Debug().Msg("Node is now Candidate")
		break
	}
	log.Debug().Msgf("State is now %s", state.String())
}

func onGainingLeaderShip(i *RpcInterface) {
	if i.ClusterState.isLeader {
		log.Debug().Msg("Node is already leader, no-op")
		return
	}
	//now leadership
	log.Info().Msg("node is now leader of the shard")
	i.ClusterState.isLeader = true
	i.ClusterState.Leader = "self"
}

func onFollowingState(i *RpcInterface) {
	if !i.ClusterState.isLeader {
		log.Debug().Msg("Node is already follower, no-op")
		return
	}
	//now leadership
	log.Info().Msg("node is now a follower of the shard")
	i.ClusterState.isLeader = false
	i.ClusterState.Leader = "nil"
}

func (r RpcInterface) GetPeers(_ context.Context, req *pb.GetPeersRequest) (*pb.GetPeersResponse, error) {
	members := r.ClusterState.Members.Members()
	arr := make([]string, len(members))
	for x := 0; x < len(arr); x++ {
		arr[x] = members[x].Name
	}
	return &pb.GetPeersResponse{Peers: arr}, nil
}
