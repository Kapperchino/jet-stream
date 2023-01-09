package cluster

import (
	"context"
	"github.com/Kapperchino/jet-application/fsm"
	pb "github.com/Kapperchino/jet-cluster/proto"
)

type RpcInterface struct {
	NodeState *fsm.NodeState
	pb.UnimplementedClusterMetaServiceServer
}

func (r RpcInterface) GetPeers(_ context.Context, req *pb.GetPeersRequest) (*pb.GetPeersResponse, error) {
	members := r.NodeState.Members.Members()
	arr := make([]string, len(members))
	for x := 0; x < len(arr); x++ {
		arr[x] = members[x].Name
	}
	return &pb.GetPeersResponse{Peers: arr}, nil
}
