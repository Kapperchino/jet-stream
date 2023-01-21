package factory

import (
	"fmt"
	raftadmin "github.com/Kapperchino/jet-admin"
	application "github.com/Kapperchino/jet-application"
	"github.com/Kapperchino/jet-application/fsm"
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/Kapperchino/jet-application/util"
	cluster "github.com/Kapperchino/jet-cluster"
	clusterPb "github.com/Kapperchino/jet-cluster/proto"
	"github.com/Kapperchino/jet-leader-rpc/leaderhealth"
	transport "github.com/Kapperchino/jet-transport"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-uuid"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
	"path/filepath"
	"strings"
)

func NewRaft(myID, myAddress string, fsm raft.FSM, bootStrap bool, raftDir string) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.ProtocolVersion = raft.ProtocolVersionMax
	c.LocalID = raft.ServerID(myID)
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006/01/02 15:04:05"}
	output.FormatLevel = func(i interface{}) string {
		return ""
	}
	c.Logger = hclog.New(&hclog.LoggerOptions{
		Name:        "raft",
		Level:       hclog.Debug,
		DisableTime: true,
		Output:      util.NewRaftLogger(output),
	})

	baseDir := filepath.Join(raftDir, myID)
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

	if bootStrap {
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

func SetupServer(raftDir string, address string, nodeName string, gossipAddress string, rootNode string, bootstrap bool) {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		log.Fatal().Msgf("failed to parse local address (%q): %v", address, err)
	}
	sock, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}

	db, _ := bbolt.Open("./testData/bolt_"+nodeName, 0666, nil)
	nodeState := &fsm.NodeState{
		Topics: db,
	}

	r, tm, err := NewRaft(nodeName, address, nodeState, bootstrap, raftDir)
	if err != nil {
		log.Fatal().Msgf("failed to start raft: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterExampleServer(s, &application.RpcInterface{
		NodeState: nodeState,
		Raft:      r,
	})
	shardId := ""
	if bootstrap {
		id, err := uuid.GenerateUUID()
		if err != nil {
			log.Fatal().Msgf("failed to create id raft: %v", err)
		}
		shardId = id
	}
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006/01/02 15:04:05"}
	output.FormatLevel = func(i interface{}) string {
		return strings.ToUpper(fmt.Sprintf("[%-4s]", i))
	}
	output.FormatFieldName = func(i interface{}) string {
		return fmt.Sprintf("%s:", i)
	}
	output.FormatFieldValue = func(i interface{}) string {
		return strings.ToUpper(fmt.Sprintf("%s", i))
	}
	output.FormatMessage = func(i interface{}) string {
		return fmt.Sprintf("[%s] %s", nodeName, i)
	}
	clusterLog := log.With().Logger().Output(output)
	clusterRpc := &cluster.RpcInterface{
		ClusterState: nil,
		Raft:         r,
		Logger:       &clusterLog,
	}
	clusterRpc.ClusterState = cluster.InitClusterState(clusterRpc, nodeName, address, shardId)

	if bootstrap {
		memberListener := cluster.InitClusterListener(clusterRpc.ClusterState)
		memberList := NewMemberList(MakeConfig(shardId, gossipAddress, memberListener), rootNode)
		clusterRpc.MemberList = memberList
	}
	clusterPb.RegisterClusterMetaServiceServer(s, clusterRpc)
	tm.Register(s)
	leaderhealth.Setup(r, s, []string{"Example", "ClusterMetaService"})
	raftadmin.Register(s, r)
	reflection.Register(s)
	if err := s.Serve(sock); err != nil {
		log.Fatal().Msgf("failed to serve: %v", err)
	}
}
