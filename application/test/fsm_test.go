package test

import (
	"context"
	raftadmin "github.com/Kapperchino/jet-admin"
	application "github.com/Kapperchino/jet-application"
	"github.com/Kapperchino/jet-application/fsm"
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/Kapperchino/jet-application/util/factory"
	"github.com/Kapperchino/jet-leader-rpc/leaderhealth"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/test/bufconn"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

type FsmTest struct {
	suite.Suite
	client pb.ExampleClient
	lis    *bufconn.Listener
	myAddr string
}

const (
	bufSize  = 1024 * 1024 * 100
	raftDir  = "./testData/raft"
	testData = "./testData"
)

func (suite *FsmTest) SetupSuite() {
	initFolders()
	suite.lis = bufconn.Listen(bufSize)
	suite.myAddr = "localhost:" + strconv.Itoa(rand.Int()%10000)
	log.Printf("Starting the server")
	go setupServer(suite.myAddr, suite.lis)
	time.Sleep(3 * time.Second)
	log.Printf("Starting the client")
	suite.client = suite.setupClient()
}

func (suite *FsmTest) TearDownSuite() {
	cleanup()
}

func (suite *FsmTest) Test_Publish() {
	log.Printf("Creating topic")
	_, err := suite.client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:         "Test_Publish",
		NumPartitions: 1,
	})
	assert.Nil(suite.T(), err)
	var arr []*pb.KeyVal
	token := make([]byte, 3*1024*1024)
	rand.Read(token)
	arr = append(arr, &pb.KeyVal{
		Key: []byte("joe"),
		Val: token,
	})
	for x := 0; x < 10; x++ {
		res, err := publishMessages(suite.client, "Test_Publish", arr, 0)
		assert.Nil(suite.T(), err)
		assert.NotNil(suite.T(), res)
	}
}

func (suite *FsmTest) Test_Publish_No_Topic() {
	var arr []*pb.KeyVal
	token := make([]byte, 3*1024*1024)
	rand.Read(token)
	arr = append(arr, &pb.KeyVal{
		Key: []byte("joe"),
		Val: token,
	})
	for x := 0; x < 10; x++ {
		_, err := publishMessages(suite.client, "Test_Publish_Err", arr, 0)
		assert.NotNil(suite.T(), err)
	}
}

func (suite *FsmTest) Test_Consume_No_Topic() {
	res, err := createConsumer(suite.client, "Test_Consume_Err")
	assert.NotNil(suite.T(), err)
	assert.Nil(suite.T(), res)
	err.Error()
}

func (suite *FsmTest) Test_Consume() {
	_, err := suite.client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:         "Test_Consume",
		NumPartitions: 1,
	})
	var arr []*pb.KeyVal
	token := make([]byte, 3*1024*1024)
	rand.Read(token)
	arr = append(arr, &pb.KeyVal{
		Key: []byte("joe"),
		Val: token,
	})
	for x := 0; x < 10; x++ {
		res, err := publishMessages(suite.client, "Test_Consume", arr, 0)
		assert.Nil(suite.T(), err)
		assert.NotNil(suite.T(), res)
	}
	res, err := createConsumer(suite.client, "Test_Consume")
	if assert.Nil(suite.T(), err) {
		res1, err := consumeMessages(suite.client, "Test_Consume", res.ConsumerId)
		assert.Nil(suite.T(), err)
		assert.Equal(suite.T(), 10, len(res1.GetMessages()))
	}
}

func TestFSMTestSuite(t *testing.T) {
	suite.Run(t, new(FsmTest))
}

func createConsumer(client pb.ExampleClient, topic string) (*pb.CreateConsumerResponse, error) {
	res, err := client.CreateConsumer(context.Background(), &pb.CreateConsumerRequest{
		Topic: topic,
	})
	return res, err
}

func consumeMessages(client pb.ExampleClient, topic string, consumerId int64) (*pb.ConsumeResponse, error) {
	res, err := client.Consume(context.Background(), &pb.ConsumeRequest{
		Topic:      topic,
		ConsumerId: consumerId,
	})
	return res, err
}

func publishMessages(client pb.ExampleClient, topic string, messages []*pb.KeyVal, partition int64) (*pb.PublishMessageResponse, error) {
	res, err := client.PublishMessages(context.Background(), &pb.PublishMessageRequest{
		Topic:     topic,
		Partition: partition,
		Messages:  messages,
	})
	return res, err
}

func setupServer(myAddr string, lis *bufconn.Listener) {
	_, _, err := net.SplitHostPort(myAddr)
	if err != nil {
		log.Fatalf("failed to parse local address (%q): %v", myAddr, err)
	}
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	db, _ := bbolt.Open("./testData/bolt", 0666, nil)
	nodeState := &fsm.NodeState{
		Topics: db,
	}

	r, tm, err := factory.NewRaft("test", myAddr, nodeState, true, raftDir)
	if err != nil {
		log.Fatalf("failed to start raft: %v", err)
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
		log.Fatalf("failed to serve: %v", err)
	}
}

func (suite *FsmTest) setupClient() pb.ExampleClient {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "Example"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	ctx := context.Background()
	maxSize := 1 * 1024 * 1024 * 1024
	conn, _ := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(suite.bufDialer),
		grpc.WithDefaultServiceConfig(serviceConfig), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(false),
			grpc.MaxCallRecvMsgSize(maxSize),
			grpc.MaxCallSendMsgSize(maxSize)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	return pb.NewExampleClient(conn)
}

func initFolders() {
	if err := os.MkdirAll(raftDir+"/test/", os.ModePerm); err != nil {
		log.Fatal(err)
	}
}

func cleanup() {
	err := os.RemoveAll(testData)
	if err != nil {
		return
	}
}

func (suite *FsmTest) bufDialer(context.Context, string) (net.Conn, error) {
	return suite.lis.Dial()
}
