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
	MemberList   *memberlist.Memberlist
	pb.UnimplementedClusterMetaServiceServer
}

type ClusterState struct {
	NodeId        string
	CurShardState *ShardState
	ClusterInfo   *haxmap.Map[string, ShardInfo]
}

type ShardState struct {
	ShardInfo  *ShardInfo
	MemberInfo *MemberInfo
	RaftChan   chan raft.Observation
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

func InitClusterState(i *RpcInterface, nodeName string, address string, shardId string) *ClusterState {
	clusterState := &ClusterState{
		ClusterInfo: haxmap.New[string, ShardInfo](),
		CurShardState: &ShardState{
			RaftChan: make(chan raft.Observation, 50),
			ShardInfo: &ShardInfo{
				shardId:   shardId,
				leader:    "",
				MemberMap: haxmap.New[string, MemberInfo](),
			},
			MemberInfo: &MemberInfo{
				nodeId:   nodeName,
				isLeader: false,
				address:  address,
			},
		},
	}
	clusterState.CurShardState.ShardInfo.MemberMap.Set(nodeName, MemberInfo{
		nodeId:   nodeName,
		isLeader: false,
		address:  address,
	})
	observer := raft.NewObserver(clusterState.CurShardState.RaftChan, false, nil)
	i.Raft.RegisterObserver(observer)
	go onRaftUpdates(clusterState.CurShardState.RaftChan, i)
	return clusterState
}

func InitClusterListener(clusterState *ClusterState) *ClusterListener {
	listener := ClusterListener{
		state: clusterState,
	}
	return &listener
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
			i.Logger.Trace().Msgf("No updates for shard %s", i.getShardId())
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
	i.getMemberInfo().isLeader = false
	i.getCurShardInfo().leader = string(update.LeaderID)
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
func (r RpcInterface) getCurShardInfo() *ShardInfo {
	return r.ClusterState.CurShardState.ShardInfo
}

func (r RpcInterface) getMemberMap() *haxmap.Map[string, MemberInfo] {
	return r.getCurShardInfo().MemberMap
}

func (r RpcInterface) getLeader() string {
	return r.getCurShardInfo().leader
}

func (r RpcInterface) getMemberInfo() *MemberInfo {
	return r.ClusterState.CurShardState.MemberInfo
}

func (r RpcInterface) getShardId() string {
	return r.getCurShardInfo().shardId
}

func (r RpcInterface) getShardState() *ShardState {
	return r.ClusterState.CurShardState
}

func (r RpcInterface) GetShardInfo(_ context.Context, req *pb.GetShardInfoRequest) (*pb.GetShardInfoResponse, error) {
	shardMap := r.getMemberMap()
	res := pb.GetShardInfoResponse{
		Info: &pb.ShardInfo{
			MemberAddressMap: map[string]*pb.MemberInfo{},
			LeaderId:         r.getLeader(),
			ShardId:          r.getShardId(),
		},
	}
	shardMap.ForEach(func(s string, info MemberInfo) bool {
		res.GetInfo().MemberAddressMap[s] = &pb.MemberInfo{
			NodeId:  info.nodeId,
			Address: info.address,
		}
		return true
	})
	return &res, nil
}

func (r RpcInterface) GetPeers(_ context.Context, req *pb.GetPeersRequest) (*pb.GetPeersResponse, error) {
	return &pb.GetPeersResponse{Peers: nil}, nil
}
