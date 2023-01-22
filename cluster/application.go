package cluster

import (
	"context"
	fsmPb "github.com/Kapperchino/jet-application/proto"
	pb "github.com/Kapperchino/jet-cluster/proto"
	"github.com/Kapperchino/jet/util"
	"github.com/alphadose/haxmap"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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
	ShardId       string
	CurShardState *ShardState
	ClusterInfo   *haxmap.Map[string, *ShardInfo]
}

type ShardState struct {
	ShardInfo  *ShardInfo
	MemberInfo *MemberInfo
	RaftChan   chan raft.Observation
}

type MemberInfo struct {
	NodeId   string
	IsLeader bool
	Address  string
}

type ShardInfo struct {
	shardId   string
	leader    string
	MemberMap *haxmap.Map[string, MemberInfo]
}

func InitClusterState(i *RpcInterface, nodeName string, address string, shardId string) *ClusterState {
	clusterState := &ClusterState{
		ClusterInfo: haxmap.New[string, *ShardInfo](),
		CurShardState: &ShardState{
			RaftChan: make(chan raft.Observation, 50),
			ShardInfo: &ShardInfo{
				shardId:   shardId,
				leader:    "",
				MemberMap: haxmap.New[string, MemberInfo](),
			},
			MemberInfo: &MemberInfo{
				NodeId:   nodeName,
				IsLeader: false,
				Address:  address,
			},
		},
	}
	clusterState.CurShardState.ShardInfo.MemberMap.Set(nodeName, MemberInfo{
		NodeId:   nodeName,
		IsLeader: false,
		Address:  address,
	})
	clusterState.ClusterInfo.Set(shardId, clusterState.CurShardState.ShardInfo)
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
		return
	}
	//add peer
	i.getMemberMap().Set(string(update.Peer.ID), MemberInfo{
		NodeId:   string(update.Peer.ID),
		IsLeader: false,
		Address:  string(update.Peer.Address),
	})

	i.Logger.Info().Msgf("Replicating peer %s", update.Peer.ID)
	input := &fsmPb.WriteOperation{
		Operation: &fsmPb.WriteOperation_AddMember{AddMember: &fsmPb.AddMember{
			NodeId:  string(update.Peer.ID),
			Address: string(update.Peer.Address),
		}},
		Code: fsmPb.Operation_ADD_MEMBER,
	}
	val, _ := util.SerializeMessage(input)
	res := i.Raft.Apply(val, time.Second)
	if err := res.Error(); err != nil {
		log.Err(err)
		return
	}
	err, isErr := res.Response().(error)
	if isErr {
		log.Err(err)
		return
	}
	_, isValid := res.Response().(*fsmPb.AddMemberResult)
	if !isValid {
		log.Err(err)
		return
	}

	i.Logger.Info().Msgf("Peer %s is added", update.Peer.ID)
}

func onLeaderUpdate(i *RpcInterface, update raft.LeaderObservation) {
	if string(update.LeaderID) == i.getMemberInfo().NodeId {
		i.Logger.Debug().Msg("Node is already leader")
		i.getCurShardInfo().leader = i.getMemberInfo().NodeId
		i.getMemberInfo().IsLeader = false
		return
	}
	//leadership update
	i.getMemberInfo().IsLeader = false
	i.getCurShardInfo().leader = string(update.LeaderID)
	_, exist := i.getMemberMap().Get(string(update.LeaderID))
	if exist {
		i.Logger.Debug().Msgf("Leader %s already exists, no-op", update.LeaderAddr)
		return
	}
	//leader added, we should try to see if we can get a list of servers to add to
	servers := i.Raft.GetConfiguration().Configuration().Servers
	for _, server := range servers {
		_, exist := i.getMemberMap().Get(string(server.ID))
		if !exist {
			i.Logger.Info().Msgf("adding server %s", server)
			i.getMemberMap().Set(string(server.ID), MemberInfo{
				NodeId:   string(server.ID),
				IsLeader: false,
				Address:  string(server.Address),
			})
		}
	}
	i.getMemberMap().Set(string(update.LeaderID), MemberInfo{
		NodeId:   string(update.LeaderID),
		IsLeader: true,
		Address:  string(update.LeaderAddr),
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
			NodeId:  info.NodeId,
			Address: info.Address,
		}
		return true
	})
	return &res, nil
}

func (r RpcInterface) GetPeers(_ context.Context, req *pb.GetPeersRequest) (*pb.GetPeersResponse, error) {
	return &pb.GetPeersResponse{Peers: nil}, nil
}
