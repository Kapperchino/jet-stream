package test

import (
	"context"
	raftadmin "github.com/Kapperchino/jet-admin"
	application "github.com/Kapperchino/jet-application"
	"github.com/Kapperchino/jet-application/fsm"
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/Kapperchino/jet-application/util"
	"github.com/Kapperchino/jet-cluster"
	clusterPb "github.com/Kapperchino/jet-cluster/proto"
	"github.com/Kapperchino/jet-leader-rpc/leaderhealth"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/hashicorp/memberlist"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/test/bufconn"
	"math/rand"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including assertion methods.
type ClusterTest struct {
	suite.Suite
	client clusterPb.ClusterMetaServiceClient
	lis    [2]*bufconn.Listener
	myAddr string
}

const (
	bufSize  = 1024 * 1024 * 100
	raftDir  = "./testData/raft"
	testData = "./testData"
)

// Make sure that VariableThatShouldStartAtFive is set to five
// before each test
func (suite *ClusterTest) SetupSuite() {
	suite.initFolders()
	suite.lis[0] = bufconn.Listen(bufSize)
	suite.lis[1] = bufconn.Listen(bufSize)
	suite.myAddr = "localhost:" + strconv.Itoa(rand.Int()%10000)
	log.Print("Starting the server")
	go suite.setupServer("localhost:8080", "nodeA", "localhost:8081", "", suite.lis[0], true)
	time.Sleep(5 * time.Second)
	go suite.setupServer("localhost:8082", "nodeB", "localhost:8083", "localhost:8081", suite.lis[1], false)
	time.Sleep(5 * time.Second)
	log.Print("Starting the client")
	suite.client = suite.setupClient(suite.lis[0])
}

func (suite *ClusterTest) TearDownSuite() {
	cleanup()
}

// All methods that begin with "Test" are run as tests within a
// suite.
func (suite *ClusterTest) TestMemberList() {
	res, err := suite.client.GetPeers(context.Background(), &clusterPb.GetPeersRequest{})
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), len(res.Peers), 2)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestClusters(t *testing.T) {
	suite.Run(t, new(ClusterTest))
}

func (suite *ClusterTest) initFolders() {
	if err := os.MkdirAll(raftDir+"/nodeA/", os.ModePerm); err != nil {
		log.Fatal().Err(err)
	}
	if err := os.Mkdir(raftDir+"/nodeB/", os.ModePerm); err != nil {
		log.Fatal().Err(err)
	}
}

func (suite *ClusterTest) setupServer(address string, nodeName string, gossipAddress string, rootNode string, lis *bufconn.Listener, bootstrap bool) {
	_, _, err := net.SplitHostPort(address)
	if err != nil {
		log.Fatal().Msgf("failed to parse local address (%q): %v", address, err)
	}
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}

	db, _ := bbolt.Open("./testData/bolt_"+nodeName, 0666, nil)
	list := NewMemberList(nodeName, rootNode, gossipAddress)
	nodeState := &fsm.NodeState{
		Topics:  db,
		Members: list,
	}

	r, tm, err := util.NewRaft(nodeName, address, nodeState, bootstrap, raftDir)
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
	})
	tm.Register(s)
	leaderhealth.Setup(r, s, []string{"Example"})
	raftadmin.Register(s, r)
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatal().Msgf("failed to serve: %v", err)
	}
}

func NewMemberList(name string, rootNode string, gossipAddress string) *memberlist.Memberlist {
	list, err := memberlist.Create(util.MakeConfig(name, gossipAddress))
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}
	if len(rootNode) != 0 {
		// Join an existing cluster by specifying at least one known member.
		_, err := list.Join([]string{rootNode})
		if err != nil {
			panic("Failed to join cluster: " + err.Error())
		}
	}
	// Ask for members of the cluster
	for _, member := range list.Members() {
		log.Printf("Member: %s %s\n", member.Name, member.Addr)
	}
	return list
}

func cleanup() {
	err := os.RemoveAll(testData)
	if err != nil {
		return
	}
}

func (suite *ClusterTest) setupClient(lis *bufconn.Listener) clusterPb.ClusterMetaServiceClient {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "Example"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	ctx := context.Background()
	maxSize := 1 * 1024 * 1024 * 1024
	conn, _ := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithDefaultServiceConfig(serviceConfig), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(false),
			grpc.MaxCallRecvMsgSize(maxSize),
			grpc.MaxCallSendMsgSize(maxSize)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	return clusterPb.NewClusterMetaServiceClient(conn)
}
