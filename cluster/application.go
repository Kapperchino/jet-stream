package cluster

import (
	"context"
	pb "github.com/Kapperchino/jet-cluster/proto"
	"github.com/alphadose/haxmap"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

type RpcInterface struct {
	Raft         *raft.Raft
	ClusterState *ClusterState
	Logger       *zerolog.Logger
	MemberList   *memberlist.Memberlist
	pb.UnimplementedClusterMetaServiceServer
}

func InitClusterState(i *RpcInterface, nodeName string, address string, shardId string) ClusterState {
	clusterState := ClusterState{
		ClusterInfo: haxmap.New[string, *ShardInfo](),
		CurShardState: &ShardState{
			RaftChan: make(chan raft.Observation, 50),
			ShardInfo: &ShardInfo{
				shardId:   shardId,
				Leader:    "",
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

func (r RpcInterface) GetShardInfo(_ context.Context, req *pb.GetShardInfoRequest) (*pb.GetShardInfoResponse, error) {
	shardMap := r.ClusterState.getMemberMap()
	res := pb.GetShardInfoResponse{
		Info: &pb.ShardInfo{
			MemberAddressMap: map[string]*pb.MemberInfo{},
			LeaderId:         r.ClusterState.getLeader(),
			ShardId:          r.ClusterState.getShardId(),
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

func (r RpcInterface) GetClusterInfo(context.Context, *pb.GetClusterInfoRequest) (*pb.GetClusterInfoResponse, error) {
	clusterMap := r.ClusterState.ClusterInfo
	res := &pb.GetClusterInfoResponse{Info: &pb.ClusterInfo{}}
	resMap := make(map[string]*pb.ShardInfo)
	clusterMap.ForEach(func(s string, info *ShardInfo) bool {
		resMap[s] = &pb.ShardInfo{
			MemberAddressMap: toProtoMemberMap(info.MemberMap),
			LeaderId:         info.Leader,
			ShardId:          info.shardId,
		}
		return true
	})
	res.Info.ShardMap = resMap
	return res, nil
}

func toProtoMemberMap(memberMap *haxmap.Map[string, MemberInfo]) map[string]*pb.MemberInfo {
	res := make(map[string]*pb.MemberInfo)
	memberMap.ForEach(func(s string, info MemberInfo) bool {
		res[s] = &pb.MemberInfo{
			NodeId:  info.NodeId,
			Address: info.Address,
		}
		return true
	})
	return res
}
