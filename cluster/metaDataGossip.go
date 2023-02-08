package cluster

import (
	"github.com/Kapperchino/jet-cluster/proto/proto"
	"github.com/Kapperchino/jet/util"
	"github.com/alphadose/haxmap"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"time"
)

type ClusterDelegate struct {
	ClusterState *ClusterState
}

func (c ClusterDelegate) Logger() *zerolog.Logger {
	return c.ClusterState.Logger
}

func (c ClusterDelegate) NodeMeta(limit int) []byte {
	shardMap := c.ClusterState.getMemberMap()
	res := proto.ShardInfo{
		MemberAddressMap: map[string]*proto.MemberInfo{},
		LeaderId:         c.ClusterState.getLeader(),
		ShardId:          c.ClusterState.getShardId(),
	}
	shardMap.ForEach(func(s string, info MemberInfo) bool {
		res.MemberAddressMap[s] = &proto.MemberInfo{
			NodeId:  info.NodeId,
			Address: info.Address,
		}
		return true
	})
	bytes, err := util.SerializeMessage(&res)
	if err != nil {
		c.Logger().Err(err)
		return nil
	}
	return bytes
}

func (c ClusterDelegate) NotifyMsg(bytes []byte) {
	c.Logger().Debug().Msgf("received %s", bytes)
}

func (c ClusterDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

func (c ClusterDelegate) LocalState(join bool) []byte {
	shardMap := c.ClusterState.getMemberMap()
	res := proto.ShardInfo{
		MemberAddressMap: map[string]*proto.MemberInfo{},
		LeaderId:         c.ClusterState.getLeader(),
		ShardId:          c.ClusterState.getShardId(),
		NodeId:           c.ClusterState.getNodeId(),
	}
	shardMap.ForEach(func(s string, info MemberInfo) bool {
		res.MemberAddressMap[s] = &proto.MemberInfo{
			NodeId:  info.NodeId,
			Address: info.Address,
		}
		return true
	})
	bytes, err := util.SerializeMessage(&res)
	if err != nil {
		c.Logger().Err(err)
		return nil
	}
	return bytes
}

func (c ClusterDelegate) MergeRemoteState(buf []byte, join bool) {
	var meta proto.ShardInfo
	err := util.DeserializeMessage(buf, &meta)
	if err != nil {
		c.Logger().Err(err).Msgf("Error deserializing message")
		return
	}
	c.Logger().Debug().Msgf("remote state is %v, join: %s", meta, join)
	//adding it to the cluster
	if join {
		//add node to shard
		if meta.ShardId == c.ClusterState.getShardId() {
			if meta.LeaderId == c.ClusterState.getLeader() || meta.LeaderId == "" && c.ClusterState.getLeader() != "" {
				raftPrt := c.ClusterState.getShardState().Raft
				//add voter
				address := meta.MemberAddressMap[meta.NodeId].Address
				future := raftPrt.AddVoter(raft.ServerID(meta.NodeId), raft.ServerAddress(address), 0, time.Second*20)
				go func() {
					err := future.Error()
					if err != nil {
						log.Err(err).Msgf("Error when adding voter")
					}
				}()
			}
			return
		}
		memberMap := haxmap.New[string, MemberInfo]()
		for key, val := range meta.MemberAddressMap {
			info := MemberInfo{
				NodeId:   val.NodeId,
				IsLeader: false,
				Address:  val.Address,
			}
			if info.NodeId == meta.LeaderId {
				info.IsLeader = true
			}
			memberMap.Set(key, info)
		}
		c.ClusterState.ClusterInfo.Set(meta.ShardId, &ShardInfo{
			shardId:   meta.ShardId,
			Leader:    meta.LeaderId,
			nodeId:    meta.NodeId,
			MemberMap: memberMap,
		})
		return
	}
	//update the map if possible
	memberMap := c.ClusterState.getMemberMap()
	//check if Leader is the same
	if meta.LeaderId != c.ClusterState.getLeader() {
		//Leader changed need to update everything
		for key, val := range meta.MemberAddressMap {
			member, exists := memberMap.Get(key)
			if exists {
				//everything is immutable other than  isleader
				if member.NodeId == meta.LeaderId {
					member.IsLeader = true
				} else {
					member.IsLeader = false
				}
			} else {
				info := MemberInfo{
					NodeId:   val.NodeId,
					IsLeader: false,
					Address:  val.Address,
				}
				if info.NodeId == meta.LeaderId {
					info.IsLeader = true
				}
				memberMap.Set(key, info)
			}
		}
		c.ClusterState.GetShardInfo().Leader = meta.LeaderId
		return
	}
}
