package test

import (
	pb "github.com/Kapperchino/jet-application/proto/proto"
	client "github.com/Kapperchino/jet-client"
	"github.com/Kapperchino/jet/factory"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"math/rand"
	"os"
	"testing"
	"time"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including assertion methods.
type ClientTestOneNodeCluster struct {
	suite.Suite
	client        *client.JetClient
	address       [3]string
	gossipAddress [3]string
	nodeName      [3]string
	servers       chan *factory.Server
}

const (
	bufSize  = 1024 * 1024 * 100
	raftDir  = "./testData/raft"
	testData = "./testData/"
)

// Make sure that VariableThatShouldStartAtFive is set to five
// before each test
func (suite *ClientTestOneNodeCluster) SetupSuite() {
	suite.initFolders()
	suite.address = [3]string{"localhost:8080", "localhost:8082", "localhost:8084"}
	suite.gossipAddress = [3]string{"localhost:8081", "localhost:8083", "localhost:8085"}
	suite.nodeName = [3]string{"nodeA", "nodeB", "nodeC"}
	suite.servers = make(chan *factory.Server, 5)
	log.Print("Starting the server")
	go factory.SetupServer(testData, raftDir, suite.address[0], suite.nodeName[0], suite.gossipAddress[0], "", suite.servers, "shardA")
	time.Sleep(5 * time.Second)
	go factory.SetupServer(testData, raftDir, suite.address[1], suite.nodeName[1], suite.gossipAddress[1], suite.gossipAddress[0], suite.servers, "shardA")
	time.Sleep(5 * time.Second)
	go factory.SetupServer(testData, raftDir, suite.address[2], suite.nodeName[2], suite.gossipAddress[2], suite.gossipAddress[0], suite.servers, "shardA")
	time.Sleep(5 * time.Second)
	log.Print("Starting the jetClient")
	jetClient, err := suite.setupClient(suite.address[0])
	assert.Nil(suite.T(), err)
	suite.client = jetClient
	//adding nodeB to nodeA as a follower
	time.Sleep(5 * time.Second)
}

func (suite *ClientTestOneNodeCluster) TearDownSuite() {
	cleanup()
}

// All methods that begin with "Test" are run as tests within a
// suite.
func (suite *ClientTestOneNodeCluster) TestCreateTopic() {
	_, err := suite.client.CreateTopic("TestCreateTopic", 3)
	assert.Nil(suite.T(), err)
}

func (suite *ClientTestOneNodeCluster) TestPublishMessage() {
	const TOPIC = "TestPublishMessage"
	_, err := suite.client.CreateTopic(TOPIC, 3)
	assert.Nil(suite.T(), err)
	token := make([]byte, 10*1024*1024)
	rand.Read(token)
	for x := 0; x < 10; x++ {
		key := make([]byte, 10*1024*1024)
		rand.Read(key)
		arr := []*pb.KeyVal{{
			Key: key,
			Val: token,
		}}
		res, err := publishMessages(suite.client, TOPIC, arr)
		assert.Nil(suite.T(), err)
		assert.NotNil(suite.T(), res)
	}
	assert.Nil(suite.T(), err)
}

func (suite *ClientTestOneNodeCluster) TestConsumeMessage() {
	const TOPIC = "TestConsumeMessage"
	_, err := suite.client.CreateTopic(TOPIC, 5)
	assert.Nil(suite.T(), err)
	token := make([]byte, 10*1024*1024)
	rand.Read(token)
	for x := 0; x < 100; x++ {
		key := make([]byte, 10*1024*1024)
		rand.Read(key)
		arr := []*pb.KeyVal{{
			Key: key,
			Val: token,
		}}
		res, err := publishMessages(suite.client, TOPIC, arr)
		assert.Nil(suite.T(), err)
		assert.NotNil(suite.T(), res)
	}
	assert.Nil(suite.T(), err)
	id, err := suite.client.CreateConsumerGroup(TOPIC)
	assert.Nil(suite.T(), err)
	messages, err := suite.client.ConsumeMessage(TOPIC, id.Id)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), 100, len(messages))
	messages, err = suite.client.ConsumeMessage(TOPIC, id.Id)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), 0, len(messages))
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestClient(t *testing.T) {
	suite.Run(t, new(ClientTestOneNodeCluster))
}

func publishMessages(client *client.JetClient, topic string, messages []*pb.KeyVal) (*pb.PublishMessageResponse, error) {
	return client.PublishMessage(messages, topic)
}

func (suite *ClientTestOneNodeCluster) initFolders() {
	if err := os.MkdirAll(raftDir+"/nodeA/", os.ModePerm); err != nil {
		log.Fatal().Err(err)
	}
	if err := os.Mkdir(raftDir+"/nodeB/", os.ModePerm); err != nil {
		log.Fatal().Err(err)
	}
}

func (suite *ClientTestOneNodeCluster) setupClient(address string) (*client.JetClient, error) {
	return client.New(address)
}

func cleanup() {
	err := os.RemoveAll(testData)
	if err != nil {
		return
	}
}
