package main

import (
	"flag"
	"github.com/Kapperchino/jet/factory"
	"github.com/rs/zerolog/log"
)

var (
	myAddr        = flag.String("address", "localhost:50051", "TCP host+port for this node")
	gossipAddress = flag.String("gossip_address", "localhost:50052", "address for gossip")
	raftId        = flag.String("raft_id", "", "Node id used by Raft")

	raftDir  = flag.String("raft_data_dir", "data/", "Raft data dir")
	dataDir  = flag.String("data_dir", "", "Local store for the partitions")
	rootNode = flag.String("root_node", "", "Root node for gossip membership")
	shardId  = flag.String("shard_id", "", "Shard id for the shard group")
)

func main() {
	flag.Parse()
	if *raftId == "" {
		log.Fatal().Msg("Cannot have null raftid")
	}
	if *shardId == "" {
		log.Fatal().Msgf("Cannot have null shardId")
	}
	channel := make(chan *factory.Server, 5)
	factory.SetupServer(*dataDir, *raftDir, *myAddr, *raftId, *gossipAddress, *rootNode, channel, *shardId)
}
