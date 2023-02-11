package main

import (
	"flag"
	"github.com/Kapperchino/jet/factory"
	"github.com/rs/zerolog/log"
	"os"
)

var (
	myAddr        = flag.String("address", "", "TCP host+port for this node")
	gossipAddress = flag.String("gossip_address", "", "address for gossip")
	raftId        = flag.String("raft_id", "", "Node id used by Raft")

	raftDir  = flag.String("raft_data_dir", "data/", "Raft data dir")
	dataDir  = flag.String("data_dir", "", "Local store for the partitions")
	rootNode = flag.String("root_node", "", "Root node for gossip membership")
	shardId  = flag.String("shard_id", "", "Shard id for the shard group")
)

func main() {
	flag.Parse()
	if *raftId == "" {
		name := os.Getenv("HOSTNAME")
		if name == "" {
			log.Fatal().Msg("Cannot have null raftid")
		}
		*raftId = name
	}
	if *myAddr == "" {
		addr := os.Getenv("POD_IP")
		if addr == "" {
			log.Fatal().Msg("Cannot have null myAddr")
		}
		*myAddr = addr + ":8080"
	}

	if *gossipAddress == "" {
		addr := os.Getenv("POD_IP")
		if addr == "" {
			log.Fatal().Msg("Cannot have null myAddr")
		}
		*gossipAddress = addr + ":8081"
	}
	if *shardId == "" {
		log.Fatal().Msgf("Cannot have null shardId")
	}
	channel := make(chan *factory.Server, 5)
	factory.SetupServer(*dataDir, *raftDir, *myAddr, *raftId, *gossipAddress, *rootNode, channel, *shardId)
}
