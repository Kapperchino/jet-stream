package test

import (
	"context"
	"github.com/Kapperchino/jet-application/util/factory"
	clusterPb "github.com/Kapperchino/jet-cluster/proto"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
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
	go factory.SetupMemServer(raftDir, "nodeA", "localhost:8080", "", suite.lis[0], true)
	time.Sleep(5 * time.Second)
	go factory.SetupMemServer(raftDir, "nodeB", "localhost:8081", "localhost:8080", suite.lis[1], false)
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
