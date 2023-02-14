package benchmark

import (
	"context"
	"github.com/Kapperchino/jet-application/proto/proto"
	client2 "github.com/Kapperchino/jet-client"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"math/rand"
	"testing"
)

const TEST_TOPIC = "test"
const MSG_SIZE = 100 * 1024

var kafkaCon *kafka.Conn
var jetClient *client2.JetClient

// Make sure that VariableThatShouldStartAtFive is set to five
// before each test
func init() {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "ubuntu/zookeeper:edge",
		ExposedPorts: []string{"2181:2181"},
		WaitingFor:   wait.ForLog("INFO ZooKeeper audit is disabled."),
	}
	_, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	req = testcontainers.ContainerRequest{
		Image:        "ubuntu/kafka:latest",
		ExposedPorts: []string{"9092:9092"},
		WaitingFor:   wait.ForLog("[KafkaServer id=0] started"),
		Env: map[string]string{"ZOOKEEPER_HOST": "host.docker.internal",
			"KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092"},
	}
	kafkaPod, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	ip, err := kafkaPod.Endpoint(ctx, "")
	if err != nil {
		log.Panic()
	}
	port, err := kafkaPod.MappedPort(ctx, "9092/tcp")
	log.Debug().Msgf("Ip:%s,port:%s", ip, port)
	con, err := kafka.DialContext(ctx, "tcp", ip)
	if err != nil {
		log.Fatal().Err(err).Msgf("failed to dail leader")
	}
	kafkaCon = con
	initJet()
}

func initJet() {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "jet:0.04",
		ExposedPorts: []string{"8080:8080", "8081:8081/tcp", "8081:8081/udp"},
		Cmd:          []string{"--raft_id", "nodeA", "--address", "0.0.0.0:8080", "--raft_data_dir", "./testData", "--gossip_address", "localhost:8081", "--shard_id", "shardA"},
		WaitingFor:   wait.ForLog("[DEBUG] Node nodeA is already Leader"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatal().Err(err)
	}
	endpoint, err := container.Endpoint(ctx, "")
	if err != nil {
		log.Fatal().Err(err)
	}
	client, err := client2.New(endpoint)
	if err != nil {
		log.Fatal().Err(err).Msgf("error connecting")
	}
	jetClient = client
}

func getMessagesKafka(count int) []kafka.Message {
	var res []kafka.Message
	for i := 0; i < count; i++ {
		token := make([]byte, MSG_SIZE)
		rand.Read(token)
		res = append(res, kafka.Message{
			Key:   []byte(uuid.NewString()),
			Value: token,
		})
	}
	return res
}

func getMessagesJet(count int) []*proto.KeyVal {
	var res []*proto.KeyVal
	for i := 0; i < count; i++ {
		token := make([]byte, MSG_SIZE)
		rand.Read(token)
		res = append(res, &proto.KeyVal{
			Key: []byte(uuid.NewString()),
			Val: token,
		})
	}
	return res
}

func BenchmarkPublishKafka_10(b *testing.B) {
	messages := getMessagesKafka(10)
	err := kafkaCon.CreateTopics(kafka.TopicConfig{
		Topic:              "BenchmarkPublishKafka_10",
		NumPartitions:      1,
		ReplicationFactor:  1,
		ReplicaAssignments: nil,
		ConfigEntries:      nil,
	})
	if err != nil {
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = kafkaCon.WriteMessages(messages...)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkPublishKafka_100(b *testing.B) {
	messages := getMessagesKafka(100)
	err := kafkaCon.CreateTopics(kafka.TopicConfig{
		Topic:              "BenchmarkPublishKafka_100",
		NumPartitions:      1,
		ReplicationFactor:  1,
		ReplicaAssignments: nil,
		ConfigEntries:      nil,
	})
	if err != nil {
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = kafkaCon.WriteMessages(messages...)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkPublishKafka_1000(b *testing.B) {
	messages := getMessagesKafka(1000)
	err := kafkaCon.CreateTopics(kafka.TopicConfig{
		Topic:              "BenchmarkPublishKafka_100",
		NumPartitions:      1,
		ReplicationFactor:  1,
		ReplicaAssignments: nil,
		ConfigEntries:      nil,
	})
	if err != nil {
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = kafkaCon.WriteMessages(messages...)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkPublishJet_10(b *testing.B) {
	messages := getMessagesJet(10)
	_, err := jetClient.CreateTopic("BenchmarkPublishJet_10", 1)
	if err != nil {
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = jetClient.PublishMessage(messages, "BenchmarkPublishJet_10")
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkPublishJet_100(b *testing.B) {
	messages := getMessagesJet(100)
	_, err := jetClient.CreateTopic("BenchmarkPublishJet_100", 1)
	if err != nil {
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = jetClient.PublishMessage(messages, "BenchmarkPublishJet_100")
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkPublishJet_1000(b *testing.B) {
	messages := getMessagesJet(1000)
	_, err := jetClient.CreateTopic("BenchmarkPublishJet_100", 1)
	if err != nil {
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = jetClient.PublishMessage(messages, "BenchmarkPublishJet_100")
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkConsumeKafka_10(b *testing.B) {
	messages := getMessagesKafka(10)
	err := kafkaCon.CreateTopics(kafka.TopicConfig{
		Topic:              "BenchmarkConsumeKafka_10",
		NumPartitions:      1,
		ReplicationFactor:  0,
		ReplicaAssignments: nil,
		ConfigEntries:      nil,
	})
	if err != nil {
		return
	}
	for i := 0; i < b.N; i++ {
		_, err = kafkaCon.WriteMessages(messages...)
		if err != nil {
			b.Error(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kafkaCon.ReadBatch(1000, 1024*1024*10)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkConsumeKafka_100(b *testing.B) {
	messages := getMessagesKafka(100)
	err := kafkaCon.CreateTopics(kafka.TopicConfig{
		Topic:              "BenchmarkConsumeKafka_100",
		NumPartitions:      1,
		ReplicationFactor:  0,
		ReplicaAssignments: nil,
		ConfigEntries:      nil,
	})
	if err != nil {
		return
	}
	for i := 0; i < b.N; i++ {
		_, err = kafkaCon.WriteMessages(messages...)
		if err != nil {
			b.Error(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kafkaCon.ReadBatch(1000, 1024*1024*100)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkConsumeJet_10(b *testing.B) {
	messages := getMessagesJet(10)
	_, err := jetClient.CreateTopic("BenchmarkConsumeJet_10", 1)
	if err != nil {
		return
	}
	for i := 0; i < b.N; i++ {
		_, err = jetClient.PublishMessage(messages, "BenchmarkConsumeJet_10")
	}
	if err != nil {
		b.Error(err)
	}
	id, err := jetClient.CreateConsumerGroup("BenchmarkConsumeJet_10")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jetClient.ConsumeMessage("BenchmarkConsumeJet_10", id.Id)
	}
}

func BenchmarkConsumeJet_100(b *testing.B) {
	messages := getMessagesJet(100)
	_, err := jetClient.CreateTopic("BenchmarkConsumeJet_100", 1)
	if err != nil {
		return
	}
	_, err = jetClient.PublishMessage(messages, "BenchmarkConsumeJet_100")
	for i := 0; i < b.N; i++ {
		_, err = jetClient.PublishMessage(messages, "BenchmarkConsumeJet_100")
	}
	if err != nil {
		b.Error(err)
	}
	id, err := jetClient.CreateConsumerGroup("BenchmarkConsumeJet_100")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jetClient.ConsumeMessage("BenchmarkConsumeJet_100", id.Id)
	}
}
