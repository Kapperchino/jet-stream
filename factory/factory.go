package factory

import (
	"fmt"
	raftadmin "github.com/Kapperchino/jet-admin"
	application "github.com/Kapperchino/jet-application"
	"github.com/Kapperchino/jet-application/fsm"
	"github.com/Kapperchino/jet-application/fsm/handlers"
	pb "github.com/Kapperchino/jet-application/proto"
	cluster "github.com/Kapperchino/jet-cluster"
	clusterPb "github.com/Kapperchino/jet-cluster/proto"
	"github.com/Kapperchino/jet-leader-rpc/leaderhealth"
	transport "github.com/Kapperchino/jet-transport"
	"github.com/Kapperchino/jet/config"
	"github.com/Kapperchino/jet/util"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-uuid"
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

func (s *Server) Kill() {
	s.Grpc.Stop()
	if s.MemberList != nil {
		s.MemberList.Shutdown()
	}
	s.Raft.Shutdown().Error()
}

func NewRaft(myID, myAddress string, fsm raft.FSM, bootStrap bool, raftDir string) (*raft.Raft, *transport.Manager, error) {
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
		Name:        "raft",
		Level:       hclog.LevelFromString(""),
		DisableTime: true,
		Output:      util.NewRaftLogger(output),
	})

	baseDir := filepath.Join(raftDir, myID)
	logDir := filepath.Join(baseDir, "logs")
	db, err := NewBadger(logDir)
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}
	ldb := BadgerLogStore{LogStore: db}

	stableDir := filepath.Join(baseDir, "stable")
	db, err = NewBadger(stableDir)
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

func SetupServer(badgerDir string, raftDir string, address string, nodeName string, gossipAddress string, rootNode string, bootstrap bool, server chan *Server) {
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

	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006/01/02 15:04:05"}
	output.FormatLevel = func(i interface{}) string {
		return fmt.Sprintf("[%s] ", nodeName) + strings.ToUpper(fmt.Sprintf("[%-4s]", i))
	}
	output.FormatFieldName = func(i interface{}) string {
		return fmt.Sprintf("%s:", i)
	}

	db, _ := NewBadger(badgerDir + nodeName + "/Meta")
	messages, _ := NewBadger("./testData/badger/" + nodeName + "/Messages")
	nodeLogger := log.Level(config.LOG_LEVEL).Output(output)
	nodeState := &fsm.NodeState{
		MetaStore:    db,
		MessageStore: messages,
		HandlerMap:   handlers.InitHandlers(),
		Logger:       &nodeLogger,
	}
	r, tm, err := NewRaft(nodeName, address, nodeState, bootstrap, raftDir)
	if err != nil {
		log.Fatal().Msgf("failed to start raft: %v", err)
	}
	s := grpc.NewServer(grpc.MaxRecvMsgSize(1 * 1024 * 1024 * 1024))
	pb.RegisterMessageServiceServer(s, &application.RpcInterface{
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
	clusterLog := log.Level(config.LOG_LEVEL).Output(output)
	clusterRpc := &cluster.RpcInterface{
		ClusterState: nil,
		Raft:         r,
		Logger:       &clusterLog,
	}
	clusterRpc.ClusterState = cluster.InitClusterState(clusterRpc, nodeName, address, shardId, bootstrap, &clusterLog)
	var memberList *memberlist.Memberlist
	if bootstrap {
		memberListener := cluster.InitClusterListener(clusterRpc.ClusterState)
		memberList = NewMemberList(MakeConfig(nodeName, shardId, gossipAddress, memberListener, cluster.ClusterDelegate{
			ClusterState: clusterRpc.ClusterState,
		}), rootNode)
		clusterRpc.MemberList = memberList
	}
	nodeState.ShardState = clusterRpc.ClusterState.CurShardState
	clusterPb.RegisterClusterMetaServiceServer(s, clusterRpc)
	tm.Register(s)
	leaderhealth.Setup(r, s, []string{"Example", "ClusterMetaService"})
	raftadmin.Register(s, r)
	reflection.Register(s)
	server <- &Server{
		Raft:       r,
		Grpc:       s,
		MemberList: memberList,
	}
	if err := s.Serve(sock); err != nil {
		log.Fatal().Msgf("failed to serve: %v", err)
	}
}
