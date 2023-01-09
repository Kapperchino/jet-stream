package test

import (
	raftadmin "github.com/Kapperchino/jet-admin"
	application "github.com/Kapperchino/jet-application"
	"github.com/Kapperchino/jet-application/fsm"
	"github.com/Kapperchino/jet-leader-rpc/leaderhealth"
	pb "github.com/Kapperchino/jet/proto"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/test/bufconn"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including assertion methods.
type ClusterTest struct {
	suite.Suite
	client pb.ExampleClient
}

// Make sure that VariableThatShouldStartAtFive is set to five
// before each test
func (suite *ClusterTest) SetupTest() {
	initFolders()
	lis = bufconn.Listen(bufSize)
	myAddr = "localhost:" + strconv.Itoa(rand.Int()%10000)
	log.Print("Starting the server")
	go setupServer()
	time.Sleep(3 * time.Second)
	log.Print("Starting the client")
	suite.client = setupClient()
}

// All methods that begin with "Test" are run as tests within a
// suite.
func (suite *ClusterTest) TestExample() {
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestExampleTestSuite(t *testing.T) {
	suite.Run(t, new(ClusterTest))
}

func (suite *ClusterTest) setupServer(address string, nodeName string) {
	_, _, err := net.SplitHostPort(address)
	if err != nil {
		log.Fatal().Msgf("failed to parse local address (%q): %v", address, err)
	}
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}

	db, _ := bbolt.Open("./testData/bolt_"+nodeName, 0666, nil)
	nodeState := &fsm.NodeState{
		Topics: db,
	}

	r, tm, err := NewRaft(nodeName, address, nodeState)
	if err != nil {
		log.Fatal().Msgf("failed to start raft: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterExampleServer(s, &application.RpcInterface{
		NodeState: nodeState,
		Raft:      r,
	})
	tm.Register(s)
	leaderhealth.Setup(r, s, []string{"Example"})
	raftadmin.Register(s, r)
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatal().Msgf("failed to serve: %v", err)
	}
}
