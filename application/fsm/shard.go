package fsm

import (
	pb "github.com/Kapperchino/jet-stream/application/proto/proto"
	cluster "github.com/Kapperchino/jet-stream/cluster"
)

func (f *NodeState) AddMember(req *pb.AddMember) (interface{}, error) {
	f.ShardState.ShardInfo.MemberMap.Set(req.NodeId, &cluster.MemberInfo{
		NodeId:   req.NodeId,
		IsLeader: false,
		Address:  req.Address,
	})
	return nil, nil
}

func (f *NodeState) RemoveMember(req *pb.RemoveMember) (interface{}, error) {
	f.ShardState.ShardInfo.MemberMap.Del(req.NodeId)
	return nil, nil
}
