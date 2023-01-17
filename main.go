package main

import (
	"flag"
	"github.com/Kapperchino/jet-application/util/factory"
	"github.com/rs/zerolog/log"
)

var (
	myAddr        = flag.String("address", "localhost:50051", "TCP host+port for this node")
	gossipAddress = flag.String("gossip_address", "localhost:50052", "address for gossip")
	raftId        = flag.String("raft_id", "", "Node id used by Raft")

	raftDir          = flag.String("raft_data_dir", "data/", "Raft data dir")
	raftBootstrap    = flag.Bool("raft_bootstrap", false, "Whether to bootstrap the Raft cluster")
	clusterBootstrap = flag.Bool("cluster_bootstrap", false, "Whether to bootstrap the Jet cluster")
	rootNode         = flag.String("root_node", "", "Root node for gossip membership")
)

func main() {
	flag.Parse()

	if *raftId == "" {
		log.Fatal().Msg("Cannot have null raftid")
	}
	factory.SetupServer(*raftDir, *myAddr, *raftId, *gossipAddress, *rootNode, *raftBootstrap)
}
