package test

import (
	"context"
	clusterPb "github.com/Kapperchino/jet-cluster/proto/proto"
	"github.com/Kapperchino/jet/factory"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"os"
	"testing"
	"time"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including assertion methods.
type ClusterTest struct {
	suite.Suite
	client   [2]clusterPb.ClusterMetaServiceClient
	address  [2]string
	nodeName [2]string
	servers  chan *factory.Server
}

const (
	bufSize  = 1024 * 1024 * 100
	raftDir  = "./testData/raft"
	testData = "./testData/"
)

// Make sure that VariableThatShouldStartAtFive is set to five
// before each test
func (suite *ClusterTest) SetupSuite() {
	suite.initFolders()
	suite.address = [2]string{"localhost:8080", "localhost:8082"}
	suite.nodeName = [2]string{"nodeA", "nodeB"}
	suite.servers = make(chan *factory.Server, 5)
	log.Print("Starting the server")
	go factory.SetupServer(testData, raftDir, suite.address[0], suite.nodeName[0], "localhost:8081", "", true, suite.servers, "shardA")
	time.Sleep(5 * time.Second)
	go factory.SetupServer(testData, raftDir, suite.address[1], suite.nodeName[1], "localhost:8083", "localhost:8081", true, suite.servers, "shardB")
	log.Print("Starting the client")
	suite.client[0] = suite.setupClient(suite.address[0])
	suite.client[1] = suite.setupClient(suite.address[1])
	//adding nodeB to nodeA as a follower
	time.Sleep(5 * time.Second)
}

func (suite *ClusterTest) TearDownSuite() {
	cleanup()
}

// All methods that begin with "Test" are run as tests within a
// suite.
func (suite *ClusterTest) TestGetClusterInfo() {
	res, err := suite.client[0].GetClusterInfo(context.Background(), &clusterPb.GetClusterInfoRequest{})
	assert.Nil(suite.T(), err)
	log.Info().Msgf("%s", res)
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

func (suite *ClusterTest) setupClient(address string) clusterPb.ClusterMetaServiceClient {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "Example"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	maxSize := 1 * 1024 * 1024 * 1024
	conn, _ := grpc.Dial(address,
		grpc.WithDefaultServiceConfig(serviceConfig), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(false),
			grpc.MaxCallRecvMsgSize(maxSize),
			grpc.MaxCallSendMsgSize(maxSize)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	return clusterPb.NewClusterMetaServiceClient(conn)
}

func cleanup() {
	err := os.RemoveAll(testData)
	if err != nil {
		return
	}
}
