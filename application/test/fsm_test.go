package test

import (
	"context"
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/Kapperchino/jet/factory"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"math/rand"
	"os"
	"testing"
	"time"
)

type FsmTest struct {
	suite.Suite
	client  pb.MessageServiceClient
	myAddr  string
	servers chan *factory.Server
}

const (
	bufSize  = 1024 * 1024 * 100
	raftDir  = "./testData/raft"
	testData = "./testData/"
)

func (suite *FsmTest) SetupSuite() {
	initFolders()
	suite.servers = make(chan *factory.Server, 5)
	suite.myAddr = "localhost:8080"
	log.Printf("Starting the server")
	go factory.SetupServer(testData, raftDir, suite.myAddr, "nodeA", "localhost:8081", "", true, suite.servers)
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
	token := make([]byte, 10*1024*1024)
	rand.Read(token)
	arr = append(arr, &pb.KeyVal{
		Key: []byte("joe"),
		Val: token,
	})
	for x := 0; x < 10; x++ {
		res, err := publishMessages(suite.client, "Test_Publish", arr, 0)
		for _, message := range res.GetMessages() {
			log.Info().Bytes("key", message.Key).Uint64("offset", message.Offset).Msgf("message")
		}
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

func (suite *FsmTest) Test_Consume_Ack() {
	const TOPIC = "Test_Consume_No_Ack"
	_, err := suite.client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:         TOPIC,
		NumPartitions: 1,
	})
	var arr []*pb.KeyVal
	token := make([]byte, 3*1024*1024)
	rand.Read(token)
	arr = append(arr, &pb.KeyVal{
		Key: []byte("joe"),
		Val: token,
	})
	for x := 0; x < 25; x++ {
		res, err := publishMessages(suite.client, TOPIC, arr, 0)
		assert.Nil(suite.T(), err)
		assert.NotNil(suite.T(), res)
	}
	res, err := createConsumer(suite.client, TOPIC)
	assert.Nil(suite.T(), err)
	msgs, err := consumeMessages(suite.client, TOPIC, res.ConsumerId)
	assert.Nil(suite.T(), err)
	for x := uint64(1); x <= 25; x++ {
		assert.Equal(suite.T(), x, msgs.Messages[x-1].Offset)
	}
	_, err = ack(suite.client, map[uint64]uint64{0: 25}, res.ConsumerId)
	assert.Nil(suite.T(), err)
	msgs, err = consumeMessages(suite.client, TOPIC, res.ConsumerId)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), 0, len(msgs.Messages))
}

// should have the same messages because of no ack, two consume calls should result in the same offsets
func (suite *FsmTest) Test_Consume_No_Ack() {
	const TOPIC = "Test_Consume_Ack"
	_, err := suite.client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:         TOPIC,
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
		res, err := publishMessages(suite.client, TOPIC, arr, 0)
		assert.Nil(suite.T(), err)
		assert.NotNil(suite.T(), res)
	}
	res, err := createConsumer(suite.client, TOPIC)
	assert.Nil(suite.T(), err)
	msgs, err := consumeMessages(suite.client, TOPIC, res.ConsumerId)
	for x := uint64(1); x <= 10; x++ {
		assert.Equal(suite.T(), x, msgs.Messages[x-1].Offset)
	}
}

func TestFSMTestSuite(t *testing.T) {
	suite.Run(t, new(FsmTest))
}

func createConsumer(client pb.MessageServiceClient, topic string) (*pb.CreateConsumerResponse, error) {
	res, err := client.CreateConsumer(context.Background(), &pb.CreateConsumerRequest{
		Topic: topic,
	})
	return res, err
}

func consumeMessages(client pb.MessageServiceClient, topic string, consumerId uint64) (*pb.ConsumeResponse, error) {
	res, err := client.Consume(context.Background(), &pb.ConsumeRequest{
		Topic:      topic,
		ConsumerId: consumerId,
	})
	return res, err
}

func publishMessages(client pb.MessageServiceClient, topic string, messages []*pb.KeyVal, partition uint64) (*pb.PublishMessageResponse, error) {
	res, err := client.PublishMessages(context.Background(), &pb.PublishMessageRequest{
		Topic:     topic,
		Partition: partition,
		Messages:  messages,
	})
	return res, err
}

func ack(client pb.MessageServiceClient, offsets map[uint64]uint64, consumerId uint64) (*pb.AckConsumeResponse, error) {
	res, err := client.AckConsume(context.Background(), &pb.AckConsumeRequest{
		Offsets: offsets,
		Id:      consumerId,
	})
	return res, err
}

func (suite *FsmTest) setupClient() pb.MessageServiceClient {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "Example"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	maxSize := 1 * 1024 * 1024 * 1024
	conn, _ := grpc.Dial(suite.myAddr,
		grpc.WithDefaultServiceConfig(serviceConfig), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(false),
			grpc.MaxCallRecvMsgSize(maxSize),
			grpc.MaxCallSendMsgSize(maxSize)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	return pb.NewMessageServiceClient(conn)
}

func initFolders() {
	if err := os.MkdirAll(raftDir+"/nodeA/", os.ModePerm); err != nil {
		log.Err(err)
	}
}

func cleanup() {
	err := os.RemoveAll(testData)
	if err != nil {
		return
	}
}
