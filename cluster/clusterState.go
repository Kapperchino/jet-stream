package cluster

import (
	"github.com/alphadose/haxmap"
	"github.com/hashicorp/raft"
)

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

func (c ClusterState) getCurShardInfo() *ShardInfo {
	return c.CurShardState.ShardInfo
}

func (c ClusterState) getMemberMap() *haxmap.Map[string, MemberInfo] {
	return c.getCurShardInfo().MemberMap
}

func (c ClusterState) getLeader() string {
	return c.getCurShardInfo().leader
}

func (c ClusterState) getMemberInfo() *MemberInfo {
	return c.CurShardState.MemberInfo
}

func (c ClusterState) getShardId() string {
	return c.getCurShardInfo().shardId
}

func (c ClusterState) getShardState() *ShardState {
	return c.CurShardState
}
