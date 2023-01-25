package cluster

import (
	"github.com/hashicorp/memberlist"
	"github.com/rs/zerolog/log"
)

type ClusterListener struct {
	state *ClusterState
}

func (c ClusterListener) NotifyJoin(node *memberlist.Node) {
	log.Info().Msgf("node %s has joined the cluster", node.Name)
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
