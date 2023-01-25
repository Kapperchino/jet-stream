package cluster

import (
	"github.com/Kapperchino/jet-cluster/proto"
	"github.com/Kapperchino/jet/util"
	"github.com/alphadose/haxmap"
	"github.com/rs/zerolog/log"
)

type ClusterDelegate struct {
	ClusterState *ClusterState
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
		log.Err(err)
		return nil
	}
	return bytes
}

func (c ClusterDelegate) NotifyMsg(bytes []byte) {
	log.Debug().Msgf("received %s", bytes)
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
		log.Err(err)
		return nil
	}
	return bytes
}

func (c ClusterDelegate) MergeRemoteState(buf []byte, join bool) {
	log.Debug().Msgf("remote state is %s, join: %s", buf, join)
	var meta proto.ShardInfo
	err := util.DeserializeMessage(buf, &meta)
	if err != nil {
		log.Err(err).Msgf("Error deserializing message")
		return
	}
	//adding it to the cluster
	if join {
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
			leader:    meta.LeaderId,
			MemberMap: memberMap,
		})
		return
	}
	//update the map if possible
	memberMap := c.ClusterState.getMemberMap()
	//check if leader is the same
	if meta.LeaderId != c.ClusterState.getLeader() {
		//leader changed need to update everything
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
		c.ClusterState.getCurShardInfo().leader = meta.LeaderId
		return
	}
}
