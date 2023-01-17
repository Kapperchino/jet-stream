package cluster

import (
	"context"
	pb "github.com/Kapperchino/jet-cluster/proto"
	"github.com/alphadose/haxmap"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
	"time"
)

type RpcInterface struct {
	Raft         *raft.Raft
	ClusterState *ClusterState
	Logger       *zerolog.Logger
	pb.UnimplementedClusterMetaServiceServer
}

type ClusterState struct {
	NodeId       string
	IsLeader     bool
	CurShardInfo *ShardInfo
	ClusterInfo  *haxmap.Map[string, ShardInfo]
	RaftChan     chan raft.Observation
	Members      *memberlist.Memberlist
}

type MemberInfo struct {
	nodeId   string
	isLeader bool
	address  string
}

type ShardInfo struct {
	shardId   string
	leader    string
	MemberMap *haxmap.Map[string, MemberInfo]
}

func InitClusterState(memberlist *memberlist.Memberlist, i *RpcInterface, nodeName string, address string, shardId string) *ClusterState {
	clusterState := &ClusterState{
		IsLeader:    false,
		ClusterInfo: haxmap.New[string, ShardInfo](),
		CurShardInfo: &ShardInfo{
			shardId:   shardId,
			leader:    "",
			MemberMap: haxmap.New[string, MemberInfo](),
		},
		RaftChan: make(chan raft.Observation, 50),
		Members:  memberlist,
	}
	clusterState.CurShardInfo.MemberMap.Set(nodeName, MemberInfo{
		nodeId:   nodeName,
		isLeader: false,
		address:  address,
	})
	observer := raft.NewObserver(clusterState.RaftChan, false, nil)
	i.Raft.RegisterObserver(observer)
	go onRaftUpdates(clusterState.RaftChan, i)
	return clusterState
}

func onRaftUpdates(raftChan chan raft.Observation, i *RpcInterface) {
	for {
		time.Sleep(time.Second * 1)
		select {
		case observable := <-raftChan:
			switch val := observable.Data.(type) {
			case raft.RequestVoteRequest:
				i.Logger.Debug().Msg("vote requested")
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
			i.Logger.Trace().Msgf("No updates for shard %s", i.ClusterState.CurShardInfo.shardId)
		}
	}

}

func onPeerUpdate(i *RpcInterface, update raft.PeerObservation) {
	if update.Removed {
		i.Logger.Info().Msgf("Peer %s is removed", update.Peer.ID)
		i.getMemberMap().Del(string(update.Peer.ID))
		return
	}
	//peer is updated
	_, exist := i.getMemberMap().Get(string(update.Peer.ID))
	if exist {
		i.Logger.Debug().Msgf("Peer %s already exists, no-op", update.Peer.ID)
	}
	//add peer
	i.getMemberMap().Set(string(update.Peer.ID), MemberInfo{
		nodeId:   string(update.Peer.ID),
		isLeader: false,
		address:  string(update.Peer.Address),
	})
	i.Logger.Info().Msgf("Peer %s is added", update.Peer.ID)
}

func onLeaderUpdate(i *RpcInterface, update raft.LeaderObservation) {
	if string(update.LeaderID) == i.ClusterState.NodeId {
		i.Logger.Debug().Msg("Node is already leader, no-op")
		return
	}
	//leadership update
	i.ClusterState.IsLeader = false
	i.ClusterState.CurShardInfo.leader = string(update.LeaderID)
	_, exist := i.getMemberMap().Get(string(update.LeaderID))
	if exist {
		i.Logger.Debug().Msgf("Leader %s already exists, no-op", update.LeaderAddr)
		return
	}
	i.getMemberMap().Set(string(update.LeaderID), MemberInfo{
		nodeId:   string(update.LeaderID),
		isLeader: true,
		address:  string(update.LeaderAddr),
	})
}

func onStateUpdate(i *RpcInterface, state raft.RaftState) {
	switch state {
	case raft.Follower:
		break
	case raft.Leader:
		break
	case raft.Shutdown:
		i.Logger.Warn().Msg("Shutting down")
		break
	case raft.Candidate:
		i.Logger.Debug().Msg("Node is now Candidate")
		break
	}
	i.Logger.Trace().Msgf("State is now %s", state.String())
}

func (r RpcInterface) getMemberMap() *haxmap.Map[string, MemberInfo] {
	return r.ClusterState.CurShardInfo.MemberMap
}

func (r RpcInterface) getLeader() string {
	return r.ClusterState.CurShardInfo.leader
}

func (r RpcInterface) getShardId() string {
	return r.ClusterState.CurShardInfo.shardId
}

func (r RpcInterface) GetShardInfo(_ context.Context, req *pb.GetShardInfoRequest) (*pb.GetShardInfoResponse, error) {
	shardMap := r.getMemberMap()
	res := pb.GetShardInfoResponse{
		Info: &pb.ShardInfo{
			MemberAddressMap: map[string]string{},
			LeaderId:         r.ClusterState.CurShardInfo.leader,
			ShardId:          r.ClusterState.CurShardInfo.shardId,
		},
	}
	shardMap.ForEach(func(s string, info MemberInfo) bool {
		res.GetInfo().MemberAddressMap[s] = info.address
		return true
	})
	return &res, nil
}

func (r RpcInterface) GetPeers(_ context.Context, req *pb.GetPeersRequest) (*pb.GetPeersResponse, error) {
	members := r.ClusterState.Members.Members()
	arr := make([]string, len(members))
	for x := 0; x < len(arr); x++ {
		arr[x] = members[x].Name
	}
	return &pb.GetPeersResponse{Peers: arr}, nil
}
