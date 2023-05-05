package cluster

import (
	"github.com/Kapperchino/jet-stream/cluster/proto/proto"
	"github.com/Kapperchino/jet-stream/util"
	"github.com/hashicorp/memberlist"
	"github.com/rs/zerolog"
)

type ClusterListener struct {
	state *ClusterState
}

func (c ClusterListener) Logger() *zerolog.Logger {
	return c.state.Logger
}

func (c ClusterListener) NotifyJoin(node *memberlist.Node) {
	c.Logger().Info().Msgf("node %s has joined the cluster", node.Name)
}

func (c ClusterListener) NotifyLeave(node *memberlist.Node) {
	c.Logger().Warn().Msgf("Shard %s has left the cluster")
	item := c.state.ClusterInfo.Get(node.Name)
	if item == nil {
		return
	}
	c.state.ClusterInfo.Del(node.Name)
}

func (c ClusterListener) NotifyUpdate(node *memberlist.Node) {
	//TODO implement me
	if node.Name == c.state.getShardId()+"/"+c.state.getNodeId() {
		return
	}
	c.Logger().Info().Msgf("node %s has been updated", node.Name)
	var meta proto.ShardInfo
	err := util.DeserializeMessage(node.Meta, &meta)
	if err != nil {
		c.Logger().Err(err).Msgf("Error deserializing message")
		return
	}
	var localShardInfo = c.state.ClusterInfo.Get(meta.ShardId)
	if localShardInfo == nil {
		c.Logger().Info().Msgf("what the heeeeeell")
		return
	}
	localShardInfo.Leader = meta.LeaderId
	localShardInfo.MemberMap.ForEach(func(key string, val *MemberInfo) bool {
		val.Address = meta.MemberAddressMap[key].Address
		val.IsLeader = val.NodeId == meta.LeaderId
		return true
	})
}

func (c ClusterListener) Find(node *memberlist.Node) {
	//TODO implement me
	panic("implement me")
}
