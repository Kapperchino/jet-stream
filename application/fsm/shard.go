package fsm

import (
	pb "github.com/Kapperchino/jet-application/proto"
	cluster "github.com/Kapperchino/jet-cluster"
)

func (f *NodeState) AddMember(req *pb.AddMember) (interface{}, error) {
	f.ShardState.ShardInfo.MemberMap.Set(req.NodeId, cluster.MemberInfo{
		NodeId:   req.NodeId,
		IsLeader: false,
		Address:  req.Address,
	})
	return nil, nil
}
