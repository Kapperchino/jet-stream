package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/Kapperchino/jet-admin"
	application "github.com/Kapperchino/jet-application"
	"github.com/Kapperchino/jet-application/fsm"
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/Kapperchino/jet-application/util"
	cluster "github.com/Kapperchino/jet-cluster"
	clusterPb "github.com/Kapperchino/jet-cluster/proto"
	"github.com/Kapperchino/jet-leader-rpc/leaderhealth"
	transport "github.com/Kapperchino/jet-transport"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"github.com/rs/zerolog/log"
	"go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
	"path/filepath"
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
		log.Fatal()
	}

	ctx := context.Background()
	_, port, err := net.SplitHostPort(*myAddr)
	if err != nil {
		log.Fatal().Msgf("failed to parse local address (%q): %v", *myAddr, err)
	}
	sock, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}

	db, err := bbolt.Open("./bolt_"+*raftId, 0666, nil)
	if err != nil {
		log.Fatal().Msgf("failed to start bolt: %v", err)
	}
	list := NewMemberList(*raftId)
	nodeState := &fsm.NodeState{
		Topics:  db,
		Members: list,
	}

	r, tm, err := NewRaft(ctx, *raftId, *myAddr, nodeState)
	if err != nil {
		log.Fatal().Msgf("failed to start raft: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterExampleServer(s, &application.RpcInterface{
		NodeState: nodeState,
		Raft:      r,
	})
	clusterPb.RegisterClusterMetaServiceServer(s, &cluster.RpcInterface{
		NodeState: nodeState,
		Raft:      r,
	})
	tm.Register(s)
	leaderhealth.Setup(r, s, []string{"Example"})
	raftadmin.Register(s, r)
	reflection.Register(s)
	if err := s.Serve(sock); err != nil {
		log.Fatal().Msgf("failed to serve: %v", err)
	}
}

func NewMemberList(name string) *memberlist.Memberlist {
	list, err := memberlist.Create(util.MakeConfig(name, *gossipAddress))
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}
	if len(*rootNode) != 0 {
		// Join an existing cluster by specifying at least one known member.
		_, err := list.Join([]string{*rootNode})
		if err != nil {
			panic("Failed to join cluster: " + err.Error())
		}
	}
	// Ask for members of the cluster
	for _, member := range list.Members() {
		log.Printf("Member: %s %s", member.Name, member.Addr)
	}
	return list
}

func NewRaft(ctx context.Context, myID, myAddress string, fsm raft.FSM) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.LogOutput = util.Logging.Writer
	c.LocalID = raft.ServerID(myID)

	baseDir := filepath.Join(*raftDir, myID)
	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "stable.dat"), err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, baseDir, err)
	}

	tm := transport.New(raft.ServerAddress(myAddress), []grpc.DialOption{grpc.WithInsecure()})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, nil, fmt.Errorf("raft.NewRaft: %v", err)
	}

	if *raftBootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(myID),
					Address:  raft.ServerAddress(myAddress),
				},
			},
		}
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, nil, fmt.Errorf("raft.Raft.BootstrapCluster: %v", err)
		}
	}

	return r, tm, nil
}
