package cluster

import (
	fsmPb "github.com/Kapperchino/jet-application/proto/proto"
	"github.com/Kapperchino/jet-stream/util"
	"github.com/hashicorp/raft"
	"time"
)

func InitClusterListener(clusterState *ClusterState) *ClusterListener {
	listener := ClusterListener{
		state: clusterState,
	}
	return &listener
}

func onRaftUpdates(raftChan chan raft.Observation, i *RpcInterface) {
	for {
		time.Sleep(time.Second * 1)
		select {
		case observable := <-raftChan:
			switch val := observable.Data.(type) {
			case raft.RequestVoteRequest:
				i.Logger.Debug().Msg("vote requested")
				break
			case raft.RaftState:
				onStateUpdate(i, val)
				break
			case raft.PeerObservation:
				onPeerUpdate(i, val)
				break
			case raft.LeaderObservation:
				onLeaderUpdate(i, val)
				break
			case raft.FailedHeartbeatObservation:
				onFailedHeartbeat(i, val)
				break
			}
		default:
			i.Logger.Trace().Msgf("No updates for shard %s", i.ClusterState.getShardId())
		}
	}

}

// TODO: update node when there's a change
func onFailedHeartbeat(i *RpcInterface, update raft.FailedHeartbeatObservation) {
	i.Logger.Warn().Msgf("Peer %s cannot be connected to, last contact: %s, nodes: %s", update.PeerID, update.LastContact.String(), i.Raft.GetConfiguration().Configuration().Servers)
	if time.Now().Sub(update.LastContact).Seconds() > 10 {
		err := i.Raft.RemoveServer(update.PeerID, i.Raft.LastIndex()-1, 0).Error()
		if err != nil {
			return
		}
		i.Logger.Info().Msgf("Peer %s is removed", update.PeerID)
		i.ClusterState.getMemberMap().Del(string(update.PeerID))
		err = RemovePeer(i, string(update.PeerID))
		if err != nil {
			i.Logger.Err(err).Msgf("Error removing peer %s", update.PeerID)
			return
		}
	}
}

func onPeerUpdate(i *RpcInterface, update raft.PeerObservation) {
	if update.Removed {
		i.Logger.Info().Msgf("Peer %s is removed", update.Peer.ID)
		i.ClusterState.getMemberMap().Del(string(update.Peer.ID))
		err := RemovePeer(i, string(update.Peer.ID))
		if err != nil {
			i.Logger.Err(err).Msgf("Error removing peer %s", update.Peer.ID)
			return
		}
		return
	}
	if len(update.Peer.ID) == 0 {
		return
	}
	//peer is updated
	_, exist := i.ClusterState.getMemberMap().Get(string(update.Peer.ID))
	if exist {
		i.Logger.Debug().Msgf("Peer %s already exists, no-op", update.Peer.ID)
		return
	}
	//add peer
	i.ClusterState.getMemberMap().Set(string(update.Peer.ID), MemberInfo{
		NodeId:   string(update.Peer.ID),
		IsLeader: false,
		Address:  string(update.Peer.Address),
	})

	i.Logger.Info().Msgf("Replicating peer %s", update.Peer.ID)
	err := ReplicatePeer(i, update)
	if err != nil {
		i.Logger.Err(err).Msgf("Error replicating peer %s", update.Peer.ID)
		return
	}
	i.Logger.Info().Msgf("Peer %s is added", update.Peer.ID)
}

func onLeaderUpdate(i *RpcInterface, update raft.LeaderObservation) {
	if string(update.LeaderID) == i.ClusterState.getMemberInfo().NodeId {
		i.Logger.Debug().Msgf("Node %s is already Leader", i.ClusterState.getMemberInfo().NodeId)
		i.ClusterState.GetShardInfo().Leader = string(update.LeaderID)
		i.ClusterState.getMemberInfo().IsLeader = true
		return
	}
	//leadership update
	i.ClusterState.getMemberInfo().IsLeader = false
	i.ClusterState.GetShardInfo().Leader = string(update.LeaderID)
	item, exist := i.ClusterState.getMemberMap().Get(string(update.LeaderID))
	if exist {
		if item.NodeId == "" {
			item.NodeId = string(update.LeaderID)
		}
		i.Logger.Debug().Msgf("Leader %s already exists, no-op", update.LeaderAddr)
		return
	}
	//Leader added, we should try to see if we can get a list of servers to add to
	servers := i.Raft.GetConfiguration().Configuration().Servers
	for _, server := range servers {
		_, exist := i.ClusterState.getMemberMap().Get(string(server.ID))
		if !exist {
			i.Logger.Info().Msgf("adding server %s", server)
			i.ClusterState.getMemberMap().Set(string(server.ID), MemberInfo{
				NodeId:   string(server.ID),
				IsLeader: false,
				Address:  string(server.Address),
			})
		}
	}
	i.ClusterState.getMemberMap().Set(string(update.LeaderID), MemberInfo{
		NodeId:   string(update.LeaderID),
		IsLeader: true,
		Address:  string(update.LeaderAddr),
	})
}

func onStateUpdate(i *RpcInterface, state raft.RaftState) {
	switch state {
	case raft.Follower:
		break
	case raft.Leader:
		break
	case raft.Shutdown:
		i.Logger.Warn().Msg("Shutting down")
		break
	case raft.Candidate:
		i.Logger.Debug().Msg("Node is now Candidate")
		break
	}
	i.Logger.Trace().Msgf("State is now %s", state.String())
}

func ReplicatePeer(i *RpcInterface, update raft.PeerObservation) error {
	input := &fsmPb.WriteOperation{
		Operation: &fsmPb.WriteOperation_AddMember{AddMember: &fsmPb.AddMember{
			NodeId:  string(update.Peer.ID),
			Address: string(update.Peer.Address),
		}},
		Code: fsmPb.Operation_ADD_MEMBER,
	}
	val, _ := util.SerializeMessage(input)
	res := i.Raft.Apply(val, time.Second)
	if err := res.Error(); err != nil {
		i.Logger.Err(err)
		return err
	}
	err, isErr := res.Response().(error)
	if isErr {
		i.Logger.Err(err)
		return err
	}
	_, isValid := res.Response().(*fsmPb.AddMemberResult)
	if !isValid {
		i.Logger.Err(err)
		return err
	}
	return nil
}

func RemovePeer(i *RpcInterface, peerId string) error {
	input := &fsmPb.WriteOperation{
		Operation: &fsmPb.WriteOperation_RemoveMember{RemoveMember: &fsmPb.RemoveMember{
			NodeId: peerId,
		}},
		Code: fsmPb.Operation_REMOVE_MEMBER,
	}
	val, _ := util.SerializeMessage(input)
	res := i.Raft.Apply(val, time.Second)
	if err := res.Error(); err != nil {
		i.Logger.Err(err)
		return err
	}
	err, isErr := res.Response().(error)
	if isErr {
		i.Logger.Err(err)
		return err
	}
	_, isValid := res.Response().(*fsmPb.RemoveMemberResult)
	if !isValid {
		i.Logger.Err(err)
		return err
	}
	return nil
}
