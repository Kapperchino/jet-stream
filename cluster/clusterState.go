package cluster

import (
	"github.com/alphadose/haxmap"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

type ClusterState struct {
	ShardId       string
	CurShardState *ShardState
	ClusterInfo   *haxmap.Map[string, *ShardInfo]
	Logger        *zerolog.Logger
}

type ShardState struct {
	ShardInfo  *ShardInfo
	MemberInfo *MemberInfo
	RaftChan   chan raft.Observation
	Raft       *raft.Raft
}

type MemberInfo struct {
	NodeId   string
	IsLeader bool
	Address  string
}

type ShardInfo struct {
	shardId   string
	nodeId    string
	Leader    string
	MemberMap *haxmap.Map[string, MemberInfo]
}

func (c ClusterState) GetShardInfo() *ShardInfo {
	return c.CurShardState.ShardInfo
}

func (c ClusterState) getMemberMap() *haxmap.Map[string, MemberInfo] {
	return c.GetShardInfo().MemberMap
}

func (c ClusterState) getLeader() string {
	return c.GetShardInfo().Leader
}

func (c ClusterState) getNodeId() string {
	return c.getMemberInfo().NodeId
}

func (c ClusterState) getMemberInfo() *MemberInfo {
	return c.CurShardState.MemberInfo
}

func (c ClusterState) getShardId() string {
	return c.GetShardInfo().shardId
}

func (c ClusterState) getShardState() *ShardState {
	return c.CurShardState
}
