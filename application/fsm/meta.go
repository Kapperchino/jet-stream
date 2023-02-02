package fsm

import (
	"github.com/Kapperchino/jet-application/proto"
	"github.com/rs/zerolog/log"
)

func (f *NodeState) GetMeta() (*proto.GetMetaResponse, error) {
	topics, err := f.getTopics()
	if err != nil {
		log.Err(err).Msgf("Error getting topics")
		return nil, err
	}
	groups, err := f.getAllConsumerGroups()
	if err != nil {
		log.Err(err).Msgf("Error getting topics")
		return nil, err
	}
	res := proto.GetMetaResponse{
		Topics:         topics,
		ConsumerGroups: groups,
	}
	return &res, nil
}
