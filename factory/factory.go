package factory

import (
	"fmt"
	application "github.com/Kapperchino/jet-stream/application"
	"github.com/Kapperchino/jet-stream/application/fsm"
	"github.com/Kapperchino/jet-stream/application/fsm/handlers"
	pb "github.com/Kapperchino/jet-stream/application/proto/proto"
	cluster "github.com/Kapperchino/jet-stream/cluster"
	clusterPb "github.com/Kapperchino/jet-stream/cluster/proto/proto"
	"github.com/Kapperchino/jet-stream/config"
	_ "github.com/Kapperchino/jet-stream/factory/vtprotoencoding"
	"github.com/Kapperchino/jet-stream/leader-rpc/leaderhealth"
	"github.com/Kapperchino/jet-stream/raftadmin"
	"github.com/Kapperchino/jet-stream/transport"
	"github.com/Kapperchino/jet-stream/util"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
	"path/filepath"
	"strings"
)

type Server struct {
	Raft       *raft.Raft
	Grpc       *grpc.Server
	MemberList *memberlist.Memberlist
}

type JetConfig struct {
	HostAddr      string
	BadgerDir     string
	RaftDir       string
	GlobalAdr     string
	NodeName      string
	GossipAddress string
	RootNode      string
	Server        chan *Server
	ShardId       string
	InMemory      bool
}

func (s *Server) Kill() {
	s.Grpc.Stop()
	if s.MemberList != nil {
		s.MemberList.Shutdown()
	}
	s.Raft.Shutdown().Error()
}

func NewRaft(myID, myAddress string, fsm raft.FSM, raftDir string, inMem bool) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.ProtocolVersion = raft.ProtocolVersionMax
	c.LocalID = raft.ServerID(myID)
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006/01/02 15:04:05"}
	output.FormatLevel = func(i interface{}) string {
		return ""
	}
	output.FormatMessage = func(i interface{}) string {
		return fmt.Sprintf("[%s] %s", myID, i)
	}
	c.Logger = hclog.New(&hclog.LoggerOptions{
		Name:       "raft",
		Level:      hclog.LevelFromString(""),
		TimeFormat: "",
		Output:     util.NewRaftLogger(output),
	})

	baseDir := filepath.Join(raftDir, myID)
	logDir := filepath.Join(baseDir, "logs")
	db, err := NewBadger(logDir, inMem)
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}
	ldb := BadgerLogStore{LogStore: db}

	stableDir := filepath.Join(baseDir, "stable")
	db, err = NewBadger(stableDir, inMem)
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}
	sdb := BadgerLogStore{LogStore: db}

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, baseDir, err)
	}

	tm := transport.New(raft.ServerAddress(myAddress), []grpc.DialOption{grpc.WithInsecure()})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, nil, fmt.Errorf("raft.NewRaft: %v", err)
	}

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
		log.Err(err).Msgf("Bootstrap error")
	}

	return r, tm, nil
}

func SetupServer(jetConfig *JetConfig) {
	_, _, err := net.SplitHostPort(jetConfig.HostAddr)
	if err != nil {
		log.Fatal().Msgf("failed to parse local GlobalAdr (%q): %v", jetConfig.HostAddr, err)
	}
	sock, err := net.Listen("tcp", jetConfig.HostAddr)
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}

	defaultOutput := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006/01/02 15:04:05"}
	defaultOutput.FormatLevel = func(i interface{}) string {
		return strings.ToUpper(fmt.Sprintf("[%-4s]", i))
	}
	defaultOutput.FormatFieldName = func(i interface{}) string {
		return fmt.Sprintf("%s:", i)
	}
	log.Logger = log.Output(defaultOutput)
	outputWithNode := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006/01/02 15:04:05"}
	outputWithNode.FormatLevel = func(i interface{}) string {
		return fmt.Sprintf("[%s] ", jetConfig.NodeName) + strings.ToUpper(fmt.Sprintf("[%-4s]", i))
	}
	outputWithNode.FormatFieldName = func(i interface{}) string {
		return fmt.Sprintf("%s:", i)
	}
	db, _ := NewBadger(jetConfig.BadgerDir+"/"+jetConfig.NodeName+"/Meta", jetConfig.InMemory)
	messages, _ := NewBadger(jetConfig.BadgerDir+"/"+jetConfig.NodeName+"/Messages", jetConfig.InMemory)
	nodeLogger := log.Level(config.LOG_LEVEL).Output(outputWithNode)
	nodeState := &fsm.NodeState{
		MetaStore:    db,
		MessageStore: messages,
		HandlerMap:   handlers.InitHandlers(),
		Logger:       &nodeLogger,
	}
	r, tm, err := NewRaft(jetConfig.NodeName, jetConfig.HostAddr, nodeState, jetConfig.RaftDir, jetConfig.InMemory)
	if err != nil {
		log.Fatal().Msgf("failed to start raft: %v", err)
	}
	s := grpc.NewServer(grpc.MaxRecvMsgSize(1 * 1024 * 1024 * 1024))
	pb.RegisterMessageServiceServer(s, &application.RpcInterface{
		NodeState: nodeState,
		Raft:      r,
	})
	clusterLog := log.Level(config.LOG_LEVEL).Output(outputWithNode)
	clusterRpc := &cluster.RpcInterface{
		ClusterState: nil,
		Raft:         r,
		Logger:       &clusterLog,
	}
	clusterRpc.ClusterState = cluster.InitClusterState(clusterRpc, jetConfig.NodeName, jetConfig.GlobalAdr, jetConfig.ShardId, &clusterLog, r)
	memberListener := cluster.InitClusterListener(clusterRpc.ClusterState)
	memberList := NewMemberList(MakeConfig(jetConfig.NodeName, jetConfig.ShardId, jetConfig.GossipAddress, memberListener, cluster.ClusterDelegate{
		ClusterState: clusterRpc.ClusterState,
	}), jetConfig.RootNode)
	clusterRpc.MemberList = memberList
	nodeState.ShardState = clusterRpc.ClusterState.CurShardState
	clusterPb.RegisterClusterMetaServiceServer(s, clusterRpc)
	tm.Register(s)
	leaderhealth.Setup(r, s, []string{"cluster.ClusterMetaService", "", "message.MessageService", "transport.RaftTransport"})
	raftadmin.Register(s, r)
	reflection.Register(s)
	jetConfig.Server <- &Server{
		Raft:       r,
		Grpc:       s,
		MemberList: memberList,
	}

	// Serve gRPC and HTTP servers concurrently.
	if err := s.Serve(sock); err != nil {
		log.Fatal().Msgf("failed to serve gRPC Server: %v", err)
	}
}
