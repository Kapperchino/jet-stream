package test

import (
	"context"
	clusterPb "github.com/Kapperchino/jet-stream/cluster/proto/proto"
	"github.com/Kapperchino/jet-stream/factory"
	adminPb "github.com/Kapperchino/jet-stream/raftadmin/proto/proto"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"os"
	"testing"
	"time"
)

// These tests are to test the raft functionalites of one shard (1 to 3 nodes)
// The cluster tests is the one that tests the entire cluster consists of many shards
type ShardsTest struct {
	suite.Suite
	client       [2]clusterPb.ClusterMetaServiceClient
	healthClient [2]grpc_health_v1.HealthClient
	adminClient  adminPb.RaftAdminClient
	address      [2]string
	nodeName     [2]string
	servers      chan *factory.Server
}

// Make sure that VariableThatShouldStartAtFive is set to five
// before each test
func (suite *ShardsTest) SetupSuite() {
	suite.initFolders()
	suite.address = [2]string{"localhost:8080", "localhost:8082"}
	suite.nodeName = [2]string{"nodeA", "nodeB"}
	suite.servers = make(chan *factory.Server, 5)
	log.Print("Starting the server")
	go factory.SetupServer(testData, raftDir, suite.address[0], suite.nodeName[0], "localhost:8081", "", suite.servers, "shardA")
	time.Sleep(5 * time.Second)
	go factory.SetupServer(testData, raftDir, suite.address[1], suite.nodeName[1], "localhost:8083", "localhost:8081", suite.servers, "shardA")
	log.Print("Starting the client")
	suite.client[0] = suite.setupClient(suite.address[0])
	suite.adminClient = suite.setupAdminClient(suite.address[0])
	suite.client[1] = suite.setupClient(suite.address[1])
	suite.healthClient[0] = suite.setupHealthClient(suite.address[0])
	time.Sleep(5 * time.Second)
}

func (suite *ShardsTest) TearDownSuite() {
	cleanup()
}

func (suite *ShardsTest) TestGetShardInfo() {
	res, err := suite.client[0].GetShardInfo(context.Background(), &clusterPb.GetShardInfoRequest{})
	log.Info().Msgf("%+v", res)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), len(res.GetInfo().MemberAddressMap), 2)
	res1, err := suite.healthClient[0].Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	log.Print(res1.GetStatus())
}

func (suite *ShardsTest) TestGetShardInfoPeer() {
	res, err := suite.client[1].GetShardInfo(context.Background(), &clusterPb.GetShardInfoRequest{})
	log.Info().Msgf("%+v", res)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "nodeA", res.GetInfo().LeaderId)
	assert.Equal(suite.T(), len(res.GetInfo().GetMemberAddressMap()), 2)
}

func (suite *ShardsTest) TestGetShardInfoPeerAddingNode() {
	res, err := suite.client[1].GetShardInfo(context.Background(), &clusterPb.GetShardInfoRequest{})
	log.Info().Msgf("%+v", res)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "nodeA", res.GetInfo().LeaderId)
	assert.Equal(suite.T(), len(res.GetInfo().GetMemberAddressMap()), 2)
	//Currently we have 2 nodes in a shard
	suite.addNodeC()
	time.Sleep(3 * time.Second)
	client := suite.setupClient("localhost:8084")
	res, err = client.GetShardInfo(context.Background(), &clusterPb.GetShardInfoRequest{})
	log.Info().Msgf("%+v", res)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "nodeA", res.GetInfo().LeaderId)
	assert.Equal(suite.T(), len(res.GetInfo().GetMemberAddressMap()), 3)
	time.Sleep(3 * time.Second)
	res, err = suite.client[1].GetShardInfo(context.Background(), &clusterPb.GetShardInfoRequest{})
	log.Info().Msgf("%+v", res)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "nodeA", res.GetInfo().LeaderId)
	assert.Equal(suite.T(), len(res.GetInfo().GetMemberAddressMap()), 3)
	server := <-suite.servers
	server.Kill()
}

func (suite *ShardsTest) TestGetShardInfoPeerDeletingNode() {
	suite.addNodeC()
	time.Sleep(3 * time.Second)
	res, err := suite.client[1].GetShardInfo(context.Background(), &clusterPb.GetShardInfoRequest{})
	log.Info().Msgf("%+v", res)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "nodeA", res.GetInfo().LeaderId)
	assert.Equal(suite.T(), len(res.GetInfo().GetMemberAddressMap()), 3)
	server := <-suite.servers
	server = <-suite.servers
	server = <-suite.servers
	server.Kill()
	//Currently we have 2 nodes in a shard
	time.Sleep(15 * time.Second)
	res, err = suite.client[0].GetShardInfo(context.Background(), &clusterPb.GetShardInfoRequest{})
	log.Info().Msgf("%+v", res)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "nodeA", res.GetInfo().LeaderId)
	assert.Equal(suite.T(), len(res.GetInfo().GetMemberAddressMap()), 2)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestShards(t *testing.T) {
	suite.Run(t, new(ShardsTest))
}

func (suite *ShardsTest) initFolders() {
	if err := os.MkdirAll(raftDir+"/nodeA/", os.ModePerm); err != nil {
		log.Fatal().Err(err)
	}
	if err := os.Mkdir(raftDir+"/nodeB/", os.ModePerm); err != nil {
		log.Fatal().Err(err)
	}
	if err := os.Mkdir(raftDir+"/nodeC/", os.ModePerm); err != nil {
		log.Fatal().Err(err)
	}
}

func (suite *ShardsTest) setupClient(address string) clusterPb.ClusterMetaServiceClient {
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

func (suite *ShardsTest) setupAdminClient(address string) adminPb.RaftAdminClient {
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
	return adminPb.NewRaftAdminClient(conn)
}

func (suite *ShardsTest) setupHealthClient(address string) grpc_health_v1.HealthClient {
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
	return grpc_health_v1.NewHealthClient(conn)
}

func (suite *ShardsTest) addNodeC() {
	go factory.SetupServer(testData, raftDir, "localhost:8084", "nodeC", "localhost:8085", "localhost:8081", suite.servers, "shardA")
}
