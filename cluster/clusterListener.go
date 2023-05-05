package cluster

import (
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
	panic("implement me")
}

func (c ClusterListener) Find(node *memberlist.Node) {
	//TODO implement me
	panic("implement me")
}
