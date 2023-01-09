package test

import (
	"context"
	"fmt"
	raftadmin "github.com/Kapperchino/jet-admin"
	application "github.com/Kapperchino/jet-application"
	"github.com/Kapperchino/jet-application/fsm"
	"github.com/Kapperchino/jet-leader-rpc/leaderhealth"
	transport "github.com/Kapperchino/jet-transport"
	pb "github.com/Kapperchino/jet/proto"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/test/bufconn"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

var (
	myAddr string
	client pb.ExampleClient
	lis    *bufconn.Listener
)

const (
	bufSize  = 1024 * 1024 * 100
	raftDir  = "./testData/raft"
	testData = "./testData"
)

func TestMain(m *testing.M) {
	initFolders()
	lis = bufconn.Listen(bufSize)
	myAddr = "localhost:" + strconv.Itoa(rand.Int()%10000)
	log.Printf("Starting the server")
	go setupServer()
	time.Sleep(3 * time.Second)
	log.Printf("Starting the client")
	client = setupClient()
	code := m.Run()
	cleanup()
	os.Exit(code)
}

func Test_Publish(t *testing.T) {
	log.Printf("Creating topic")
	_, err := client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:         "Test_Publish",
		NumPartitions: 1,
	})
	assert.Nil(t, err)
	var arr []*pb.KeyVal
	token := make([]byte, 3*1024*1024)
	rand.Read(token)
	arr = append(arr, &pb.KeyVal{
		Key: []byte("joe"),
		Val: token,
	})
	for x := 0; x < 10; x++ {
		res, err := publishMessages("Test_Publish", arr, 0)
		assert.Nil(t, err)
		assert.NotNil(t, res)
	}
}

func Test_Publish_No_Topic(t *testing.T) {
	var arr []*pb.KeyVal
	token := make([]byte, 3*1024*1024)
	rand.Read(token)
	arr = append(arr, &pb.KeyVal{
		Key: []byte("joe"),
		Val: token,
	})
	for x := 0; x < 10; x++ {
		_, err := publishMessages("Test_Publish_Err", arr, 0)
		assert.NotNil(t, err)
	}
}

func Test_Consume_No_Topic(t *testing.T) {
	res, err := createConsumer("Test_Consume_Err")
	assert.NotNil(t, err)
	assert.Nil(t, res)
	err.Error()
}

func Test_Consume(t *testing.T) {
	_, err := client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
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
		res, err := publishMessages("Test_Consume", arr, 0)
		assert.Nil(t, err)
		assert.NotNil(t, res)
	}
	res, err := createConsumer("Test_Consume")
	if assert.Nil(t, err) {
		res1, err := consumeMessages("Test_Consume", res.ConsumerId)
		assert.Nil(t, err)
		assert.Equal(t, 10, len(res1.GetMessages()))
	}
}

func createConsumer(topic string) (*pb.CreateConsumerResponse, error) {
	res, err := client.CreateConsumer(context.Background(), &pb.CreateConsumerRequest{
		Topic: topic,
	})
	return res, err
}

func consumeMessages(topic string, consumerId int64) (*pb.ConsumeResponse, error) {
	res, err := client.Consume(context.Background(), &pb.ConsumeRequest{
		Topic:      topic,
		ConsumerId: consumerId,
	})
	return res, err
}

func publishMessages(topic string, messages []*pb.KeyVal, partition int64) (*pb.PublishMessageResponse, error) {
	res, err := client.PublishMessages(context.Background(), &pb.PublishMessageRequest{
		Topic:     topic,
		Partition: partition,
		Messages:  messages,
	})
	return res, err
}

func setupServer() {
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

	r, tm, err := NewRaft("test", myAddr, nodeState)
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

func setupClient() pb.ExampleClient {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "Example"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	ctx := context.Background()
	maxSize := 1 * 1024 * 1024 * 1024
	conn, _ := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(bufDialer),
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

func NewRaft(myID, myAddress string, fsm raft.FSM) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(myID)

	baseDir := filepath.Join(raftDir, myID)
	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "stable.dat"), err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, baseDir, err)
	}

	tm := transport.New(raft.ServerAddress(myAddress), []grpc.DialOption{grpc.WithInsecure()})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, nil, fmt.Errorf("raft.NewRaft: %v", err)
	}

	cfg := raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(myID),
				Address:  raft.ServerAddress(myAddress),
			},
		},
	}
	f := r.BootstrapCluster(cfg)
	if err := f.Error(); err != nil {
		return nil, nil, fmt.Errorf("raft.Raft.BootstrapCluster: %v", err)
	}
	return r, tm, nil
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}
