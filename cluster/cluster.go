package cluster

import (
	"context"
	"github.com/Kapperchino/jet-cluster/proto"
	"github.com/Kapperchino/jet-factory/clientFactory"
	"github.com/alphadose/haxmap"
	"github.com/hashicorp/memberlist"
	"github.com/rs/zerolog/log"
)

type ClusterListener struct {
	state *ClusterState
}

func (c ClusterListener) NotifyJoin(node *memberlist.Node) {
	_, exist := c.state.ClusterInfo.Get(node.Name)
	if exist {
		log.Debug().Msgf("Shard already in cluster, no-op")
		return
	}
	//Call the shard to get information then add to the state
	address := node.Addr.String() + ":8080"
	client, err := clientFactory.CreateClusterClient(address)
	if err != nil {
		log.Err(err).Msgf("Error creating Client for Cluster")
		return
	}
	info, err := client.GetClusterInfo(context.Background(), &proto.GetClusterInfoRequest{})
	if err != nil {
		log.Err(err).Msgf("Cannot connect to the grpc server for cluster")
		return
	}
	for key, val := range info.GetInfo().GetShardMap() {
		memberMap := haxmap.New[string, MemberInfo]()
		for s, s2 := range val.MemberAddressMap {
			info := MemberInfo{
				NodeId:   s2.NodeId,
				IsLeader: false,
				Address:  s2.Address,
			}
			if info.NodeId == val.LeaderId {
				info.IsLeader = true
			}
			memberMap.Set(s, info)
		}
		c.state.ClusterInfo.Set(key, &ShardInfo{
			shardId:   val.ShardId,
			leader:    val.LeaderId,
			MemberMap: memberMap,
		})
	}
}

func (c ClusterListener) NotifyLeave(node *memberlist.Node) {
	log.Warn().Msgf("Shard %s has left the cluster")
	_, exist := c.state.ClusterInfo.Get(node.Name)
	if !exist {
		return
	}
	c.state.ClusterInfo.Del(node.Name)
}

func (c ClusterListener) NotifyUpdate(node *memberlist.Node) {
	//TODO implement me
	panic("implement me")
}

func (c ClusterListener) Find(node *memberlist.Node) {
	//TODO implement me
	panic("implement me")
}
