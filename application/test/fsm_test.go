package test

import (
	"context"
	pb "github.com/Kapperchino/jet-application/proto/proto"
	"github.com/Kapperchino/jet/factory"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	vtgrpc "github.com/planetscale/vtprotobuf/codec/grpc"
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
	go factory.SetupServer(testData, raftDir, suite.myAddr, "nodeA", "localhost:8081", "", "shardA")
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
		Topic:      "Test_Publish",
		Partitions: []uint64{0},
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

func (suite *FsmTest) Test_Publish_Two_Partitions() {
	log.Printf("Creating topic")
	_, err := suite.client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:      "Test_Publish_Two_Partitions",
		Partitions: []uint64{0, 1},
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
		res, err := publishMessages(suite.client, "Test_Publish_Two_Partitions", arr, 0)
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
	res, err := createConsumerGroup(suite.client, "Test_Consume_Err")
	assert.NotNil(suite.T(), err)
	assert.Nil(suite.T(), res)
	err.Error()
}

func (suite *FsmTest) Test_Consume_Ack() {
	const TOPIC = "Test_Consume_Ack"
	_, err := suite.client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:      TOPIC,
		Partitions: []uint64{0},
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
	res, err := createConsumerGroup(suite.client, TOPIC)
	assert.Nil(suite.T(), err)
	msgs, err := consumeMessages(suite.client, TOPIC, res.Id)
	assert.Nil(suite.T(), err)
	for x := uint64(1); x <= 25; x++ {
		assert.Equal(suite.T(), x, msgs.Messages[0].Messages[x-1].Offset)
	}
	_, err = ack(suite.client, map[uint64]uint64{0: 25}, res.Id, TOPIC)
	assert.Nil(suite.T(), err)
	msgs, err = consumeMessages(suite.client, TOPIC, res.Id)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), 0, len(msgs.Messages[0].Messages))
}

// should have the same messages because of no ack, two consume calls should result in the same offsets
func (suite *FsmTest) Test_Consume_No_Ack() {
	const TOPIC = "Test_Consume_No_Ack"
	_, err := suite.client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:      TOPIC,
		Partitions: []uint64{0},
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
	res, err := createConsumerGroup(suite.client, TOPIC)
	assert.Nil(suite.T(), err)
	msgs, err := consumeMessages(suite.client, TOPIC, res.Id)
	for x := uint64(1); x <= 10; x++ {
		assert.Equal(suite.T(), x, msgs.Messages[0].Messages[x-1].Offset)
	}
}

func (suite *FsmTest) Test_Consume_Ack_Two_Partitions() {
	const TOPIC = "Test_Consume_Ack_Two_Partitions"
	_, err := suite.client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:      TOPIC,
		Partitions: []uint64{0, 1},
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
		res, err = publishMessages(suite.client, TOPIC, arr, 1)
		assert.Nil(suite.T(), err)
		assert.NotNil(suite.T(), res)
	}
	res, err := createConsumerGroup(suite.client, TOPIC)
	assert.Nil(suite.T(), err)
	msgs, err := consumeMessages(suite.client, TOPIC, res.Id)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), 25, len(msgs.Messages[0].Messages))
	assert.Equal(suite.T(), 25, len(msgs.Messages[1].Messages))
	_, err = ack(suite.client, map[uint64]uint64{0: 25, 1: 25}, res.Id, TOPIC)
	assert.Nil(suite.T(), err)
	msgs, err = consumeMessages(suite.client, TOPIC, res.Id)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), 0, len(msgs.Messages[0].Messages))
	assert.Equal(suite.T(), 0, len(msgs.Messages[1].Messages))
}

func (suite *FsmTest) Test_GetConsumerGroups() {
	const TOPIC = "Test_GetConsumerGroups"
	_, err := suite.client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:      TOPIC,
		Partitions: []uint64{0},
	})
	assert.Nil(suite.T(), err)
	var arr []*pb.CreateConsumerGroupResponse
	for i := 0; i < 5; i++ {
		res, err := createConsumerGroup(suite.client, TOPIC)
		assert.Nil(suite.T(), err)
		arr = append(arr, res)
	}
	assert.Equal(suite.T(), len(arr), 5)
}

func (suite *FsmTest) Test_GetMeta() {
	const TOPIC = "Test_GetMeta"
	_, err := suite.client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:      TOPIC,
		Partitions: []uint64{0},
	})
	assert.Nil(suite.T(), err)
	var arr []*pb.CreateConsumerGroupResponse
	for i := 0; i < 5; i++ {
		res, err := createConsumerGroup(suite.client, TOPIC)
		assert.Nil(suite.T(), err)
		arr = append(arr, res)
	}
	assert.Equal(suite.T(), len(arr), 5)
	meta, err := getMeta(suite.client)
	assert.Nil(suite.T(), err)
	assert.NotZero(suite.T(), len(meta.ConsumerGroups))
	assert.NotZero(suite.T(), len(meta.Topics))
	assert.Equal(suite.T(), meta.Topics[TOPIC].Name, TOPIC)
	assert.Equal(suite.T(), len(meta.Topics[TOPIC].Partitions), 1)
}

func TestFSMTestSuite(t *testing.T) {
	suite.Run(t, new(FsmTest))
}

func getMeta(client pb.MessageServiceClient) (*pb.GetMetaResponse, error) {
	res, err := client.GetMeta(context.Background(), &pb.GetMetaRequest{})
	return res, err
}

func createConsumerGroup(client pb.MessageServiceClient, topic string) (*pb.CreateConsumerGroupResponse, error) {
	res, err := client.CreateConsumerGroup(context.Background(), &pb.CreateConsumerGroupRequest{
		Topic: topic,
	})
	return res, err
}

func consumeMessages(client pb.MessageServiceClient, topic string, groupId string) (*pb.ConsumeResponse, error) {
	res, err := client.Consume(context.Background(), &pb.ConsumeRequest{
		Topic:   topic,
		GroupId: groupId,
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

func ack(client pb.MessageServiceClient, offsets map[uint64]uint64, groupId string, topic string) (*pb.AckConsumeResponse, error) {
	res, err := client.AckConsume(context.Background(), &pb.AckConsumeRequest{
		Offsets: offsets,
		GroupId: groupId,
		Topic:   topic,
	})
	return res, err
}

func (suite *FsmTest) setupClient() pb.MessageServiceClient {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "Example"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(0),
	}
	maxSize := 1 * 1024 * 1024 * 1024
	conn, err := grpc.Dial(suite.myAddr,
		grpc.WithDefaultServiceConfig(serviceConfig), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true),
			grpc.MaxCallRecvMsgSize(maxSize),
			grpc.MaxCallSendMsgSize(maxSize),
			grpc.CallContentSubtype(vtgrpc.Name)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	if err != nil {
		log.Err(err).Stack().Msgf("Error creating connection")
		return nil
	}
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
