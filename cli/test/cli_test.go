package test

import (
	"github.com/Kapperchino/jet-stream/factory"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli/v2"
	"os"
	"testing"
)

const (
	bufSize  = 1024 * 1024 * 100
	raftDir  = "./testData/raft"
	testData = "./testData/"
)

// These tests are to test the raft functionalites of one shard (1 to 3 nodes)
// The cluster tests is the one that tests the entire cluster consists of many shards
type cliTest struct {
	suite.Suite
	cli      *cli.App
	address  string
	nodeName string
	servers  chan *factory.Server
}

// Make sure that VariableThatShouldStartAtFive is set to five
// before each test
func (suite *cliTest) SetupSuite() {
	suite.initFolders()
	suite.address = "localhost:8080"
	suite.nodeName = "nodeA"
	suite.servers = make(chan *factory.Server, 5)
	log.Print("Starting the server")
	go factory.SetupServer(suite.address, testData, raftDir, suite.address, suite.nodeName, "localhost:8081", "", suite.servers, "shardA")
	log.Print("Starting the client")
}

func (suite *cliTest) TearDownSuite() {
	cleanup()
}

func (suite *cliTest) TestPublish() {
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestShards(t *testing.T) {
	suite.Run(t, new(cliTest))
}

func (suite *cliTest) initFolders() {
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

func cleanup() {
	err := os.RemoveAll(testData)
	if err != nil {
		return
	}
}
