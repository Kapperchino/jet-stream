package test

import (
	"crypto/rand"
	pb "github.com/Kapperchino/jet-stream/application/proto/proto"
	"github.com/Kapperchino/jet-stream/client"
	"github.com/Kapperchino/jet-stream/factory"
	"github.com/Kapperchino/jet-stream/util"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"time"
)

type ClientTestOneNodeCluster struct {
	suite.Suite
	client        *client.JetClient
	address       string
	gossipAddress string
	nodeName      string
	servers       chan *factory.Server
}

func (suite *ClientTestOneNodeCluster) SetupSuite() {
	suite.address = "localhost:8080"
	suite.gossipAddress = "localhost:8081"
	suite.nodeName = "nodeA"
	suite.servers = make(chan *factory.Server, 5)
	log.Print("Starting the server")
	go factory.SetupServer(
		&factory.JetConfig{
			HostAddr:      suite.address,
			BadgerDir:     "",
			RaftDir:       "",
			GlobalAdr:     suite.address,
			NodeName:      suite.nodeName,
			GossipAddress: suite.gossipAddress,
			RootNode:      "",
			Server:        suite.servers,
			ShardId:       "shardA",
			InMemory:      true,
		})
	time.Sleep(5 * time.Second)
	jetClient, err := suite.setupClient(suite.address)
	assert.Nil(suite.T(), err)
	suite.client = jetClient
	//adding nodeB to nodeA as a follower
}

func (suite *ClientTestOneNodeCluster) TestPublishMessage() {
	const TOPIC = "TestPublishMessage"
	_, err := suite.client.CreateTopic(TOPIC, 3)
	assert.Nil(suite.T(), err)
	token := make([]byte, 1024*128)
	_, _ = rand.Read(token)
	keys := []int64{1, 2234, 338893}
	for x := 0; x < 10; x++ {
		key := make([]byte, 1024*128)
		_, _ = rand.Read(token)
		_, _ = rand.Read(key)
		arr := []*pb.KeyVal{{
			Key: util.LongToBytes(keys[x%3]),
			Val: token,
		}}
		res, err := suite.client.PublishMessage(arr, TOPIC)
		assert.Nil(suite.T(), err)
		assert.NotNil(suite.T(), res)
		for _, message := range res.Messages {
			log.Info().Msgf("%v,%v", message.Offset, message.Partition)
		}
	}
	assert.Nil(suite.T(), err)
}

func (suite *ClientTestOneNodeCluster) TestConsumeMessage() {
	const TOPIC = "TestConsumeMessage"
	_, err := suite.client.CreateTopic(TOPIC, 5)
	assert.Nil(suite.T(), err)
	token := make([]byte, 128*1024)
	rand.Read(token)
	for x := 0; x < 100; x++ {
		key := make([]byte, 128*1024)
		rand.Read(key)
		arr := []*pb.KeyVal{{
			Key: key,
			Val: token,
		}}
		res, err := suite.client.PublishMessage(arr, TOPIC)
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

func (suite *ClientTestOneNodeCluster) setupClient(address string) (*client.JetClient, error) {
	return client.New(address)
}
