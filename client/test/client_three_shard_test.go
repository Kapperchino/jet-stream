package test

import (
	"crypto/rand"
	pb "github.com/Kapperchino/jet-stream/application/proto/proto"
	client "github.com/Kapperchino/jet-stream/client"
	"github.com/Kapperchino/jet-stream/factory"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including assertion methods.
type ClientTestThreeShardCluster struct {
	suite.Suite
	client        *client.JetClient
	address       [3]string
	gossipAddress [3]string
	nodeName      [3]string
	servers       chan *factory.Server
}

// Make sure that VariableThatShouldStartAtFive is set to five
// before each test
func (suite *ClientTestThreeShardCluster) SetupSuite() {
	suite.address = [3]string{"localhost:8080", "localhost:8082", "localhost:8084"}
	suite.gossipAddress = [3]string{"localhost:8081", "localhost:8083", "localhost:8085"}
	suite.nodeName = [3]string{"nodeA", "nodeB", "nodeC"}
	suite.servers = make(chan *factory.Server, 5)
	log.Print("Starting the server")
	go factory.SetupServer(
		&factory.JetConfig{
			HostAddr:      suite.address[0],
			BadgerDir:     "",
			RaftDir:       "",
			GlobalAdr:     suite.address[0],
			NodeName:      suite.nodeName[0],
			GossipAddress: suite.gossipAddress[0],
			RootNode:      "",
			Server:        suite.servers,
			ShardId:       "shardA",
			InMemory:      true,
		})
	time.Sleep(5 * time.Second)
	go factory.SetupServer(
		&factory.JetConfig{
			HostAddr:      suite.address[1],
			BadgerDir:     "",
			RaftDir:       "",
			GlobalAdr:     suite.address[1],
			NodeName:      suite.nodeName[1],
			GossipAddress: suite.gossipAddress[1],
			RootNode:      suite.gossipAddress[0],
			Server:        suite.servers,
			ShardId:       "shardB",
			InMemory:      true,
		})
	time.Sleep(5 * time.Second)
	go factory.SetupServer(
		&factory.JetConfig{
			HostAddr:      suite.address[2],
			BadgerDir:     "",
			RaftDir:       "",
			GlobalAdr:     suite.address[2],
			NodeName:      suite.nodeName[2],
			GossipAddress: suite.gossipAddress[2],
			RootNode:      suite.gossipAddress[0],
			Server:        suite.servers,
			ShardId:       "shardC",
			InMemory:      true,
		})
	time.Sleep(5 * time.Second)
	log.Print("Starting the jetClient")
	jetClient, err := suite.setupClient(suite.address[0])
	assert.Nil(suite.T(), err)
	suite.client = jetClient
	//adding nodeB to nodeA as a follower
	time.Sleep(5 * time.Second)
}

func (suite *ClientTestThreeShardCluster) TestPublishMessage() {
	const TOPIC = "TestPublishMessage"
	_, err := suite.client.CreateTopic(TOPIC, 3)
	assert.Nil(suite.T(), err)
	token := make([]byte, 1024)
	var arr []*pb.KeyVal
	for x := 0; x < 100; x++ {
		key := make([]byte, 16)
		rand.Read(key)
		rand.Read(token)
		arr = append(arr, &pb.KeyVal{
			Key: key,
			Val: token,
		})
	}
	res, err := publishMessages(suite.client, TOPIC, arr)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), res)
}

func (suite *ClientTestThreeShardCluster) TestConsumeMessage() {
	const TOPIC = "TestConsumeMessage"
	_, err := suite.client.CreateTopic(TOPIC, 3)
	assert.Nil(suite.T(), err)
	token := make([]byte, 10*1024)
	var arr []*pb.KeyVal
	for x := 0; x < 100; x++ {
		key := make([]byte, 10*1024)
		rand.Read(key)
		rand.Read(token)
		arr = append(arr, &pb.KeyVal{
			Key: key,
			Val: token,
		})
	}
	res, err := publishMessages(suite.client, TOPIC, arr)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), res)
	id, err := suite.client.CreateConsumerGroup(TOPIC)
	assert.Nil(suite.T(), err)
	messages, err := suite.client.ConsumeMessage(TOPIC, id.Id)
	for len(messages) == 0 {
		messages, err = suite.client.ConsumeMessage(TOPIC, id.Id)
		assert.Nil(suite.T(), err)
	}
	assert.Equal(suite.T(), 100, len(messages))
	messages, err = suite.client.ConsumeMessage(TOPIC, id.Id)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), 0, len(messages))
}

func TestThreeShard(t *testing.T) {
	suite.Run(t, new(ClientTestThreeShardCluster))
}

func (suite *ClientTestThreeShardCluster) setupClient(address string) (*client.JetClient, error) {
	return client.New(address)
}
