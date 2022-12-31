// Binary hammer sends requests to your Raft cluster as fast as it can.
// It sends the written out version of the Dutch numbers up to 2000.
// In the end it asks the Raft cluster what the longest three words were.
package main

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	_ "github.com/Jille/grpc-multi-resolver"
	pb "github.com/Kapperchino/jet/proto"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/health"
)

func main() {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "Example"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	maxSize := 1 * 1024 * 1024 * 1024
	conn, err := grpc.Dial("multi:///localhost:8080,localhost:8081,localhost:8082",
		grpc.WithDefaultServiceConfig(serviceConfig), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true),
			grpc.MaxCallRecvMsgSize(maxSize),
			grpc.MaxCallSendMsgSize(maxSize)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	if err != nil {
		log.Fatalf("dialing failed: %v", err)
	}
	defer conn.Close()
	c := pb.NewExampleClient(conn)

	_, err = c.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:         "Joe",
		NumPartitions: 2,
	})
	if err != nil {
		return
	}
	//publishTest(c)
	consumerTest(c)
}

func publishTest(c pb.ExampleClient) {
	var wg sync.WaitGroup
	for i := 0; 1 > i; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var arr []*pb.KeyVal
			token := make([]byte, 3*1024*1024)
			rand.Read(token)
			arr = append(arr, &pb.KeyVal{
				Key: []byte("joe"),
				Val: token,
			})
			for x := 0; x < 100; x++ {
				res, err := c.PublishMessages(context.Background(), &pb.PublishMessageRequest{
					Topic:     "Joe",
					Partition: 0,
					Messages:  arr,
				})
				for _, m := range res.Messages {
					log.Printf("%d", m.Offset)
				}
				if err != nil {
					log.Fatalf("AddWord RPC failed: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	log.Printf("finished publishing")
}

func consumerTest(c pb.ExampleClient) {
	id, err := c.CreateConsumer(context.Background(), &pb.CreateConsumerRequest{Topic: "Joe"})
	if err != nil {
		log.Fatalf("dialing failed: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; 1 > i; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var arr []*pb.KeyVal
			token := make([]byte, 3*1024*1024)
			rand.Read(token)
			arr = append(arr, &pb.KeyVal{
				Key: []byte("joe"),
				Val: token,
			})
			for x := 0; x < 1; x++ {
				res, err := c.Consume(context.Background(), &pb.ConsumeRequest{
					Topic:      "Joe",
					ConsumerId: id.GetConsumerId(),
				})
				for _, m := range res.Messages {
					log.Printf("%d", m.Offset)
				}
				if err != nil {
					log.Fatalf("AddWord RPC failed: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	log.Printf("finished publishing")
}
