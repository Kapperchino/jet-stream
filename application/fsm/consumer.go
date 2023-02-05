package fsm

import (
	"fmt"
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/Kapperchino/jet/util"
	"github.com/dgraph-io/badger/v3"
	"github.com/google/uuid"
	_ "github.com/rs/zerolog/log"
)

// CreateConsumerGroup write operation, done in fsm
func (f *NodeState) CreateConsumerGroup(req *pb.CreateConsumerGroup) (interface{}, error) {
	topic, err := f.getTopic(req.GetTopic())
	if err != nil {
		return nil, err
	}
	response := new(pb.CreateConsumerGroupResponse)
	err = f.MetaStore.Update(func(tx *badger.Txn) error {
		group := &pb.ConsumerGroup{
			Id:        req.Id,
			Consumers: make(map[string]*pb.Consumer),
		}
		for num, _ := range topic.Partitions {
			consumer := &pb.Consumer{
				Id:        uuid.NewString(),
				Partition: num,
				Offset:    0,
			}
			group.Consumers[consumer.Id] = consumer
		}
		buf, err := util.SerializeMessage(group)
		if err != nil {
			f.Logger.Printf("error encoding consumer group, %s", err)
			return fmt.Errorf("error encoding Topic, %w", err)
		}
		err = tx.Set([]byte("ConsumerGroup-"+topic.Name+"-"+group.Id), buf)
		if err != nil {
			f.Logger.Printf("error putting items in bucket, %s", err)
			return fmt.Errorf("error putting items in bucket, %w", err)
		}
		response.Group = group
		response.Id = group.Id
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error with local store %w", err)
	}
	return response, nil
}

// Consume operation, should be done in replicas and not in fsm
func (f *NodeState) Consume(req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	res := pb.ConsumeResponse{
		Messages:  make([]*pb.Message, 0),
		LastIndex: 0,
	}
	err := f.MessageStore.View(func(tx *badger.Txn) error {
		group, err := f.getConsumerGroup(req.GetGroupId(), req.Topic)
		if err != nil {
			f.Logger.Err(err).Msgf("Error getting consumer group")
			return err
		}
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := tx.NewIterator(opts)

		defer it.Close()

		for _, val := range group.Consumers {
			var buf []*pb.Message
			prefix := makePrefix(req.Topic, val.Partition)
			key := makeKey(req.GetTopic(), val.Partition, val.Offset+1)
			for it.Seek(key); it.ValidForPrefix(prefix); it.Next() {
				//now we need to seek until the message is found
				item := it.Item()
				err := item.Value(func(v []byte) error {
					var message pb.Message
					err := util.DeserializeMessage(v, &message)
					if err != nil {
						return err
					}
					buf = append(buf, &message)
					return nil
				})
				if err != nil {
					return err
				}
			}
			res.Messages = append(res.Messages, buf...)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error with local store %w", err)
	}
	return &res, nil
}

func (f *NodeState) GetConsumerGroups(topic string) (*pb.GetConsumerGroupsResponse, error) {
	groups := map[string]*pb.ConsumerGroup{}
	err := f.MetaStore.View(func(tx *badger.Txn) error {
		prefix := []byte("ConsumerGroup-" + topic)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := tx.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(v []byte) error {
				var group pb.ConsumerGroup
				err := util.DeserializeMessage(v, &group)
				if err != nil {
					return err
				}
				groups[group.GetId()] = &group
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error with local store, %s", err)
	}
	return &pb.GetConsumerGroupsResponse{Groups: groups}, nil
}

func (f *NodeState) getAllConsumerGroups() (map[string]*pb.ConsumerGroup, error) {
	groups := map[string]*pb.ConsumerGroup{}
	err := f.MetaStore.View(func(tx *badger.Txn) error {
		prefix := []byte("ConsumerGroup-")
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := tx.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(v []byte) error {
				var group pb.ConsumerGroup
				err := util.DeserializeMessage(v, &group)
				if err != nil {
					return err
				}
				groups[group.GetId()] = &group
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error with local store, %s", err)
	}
	return groups, nil
}

func (f *NodeState) getConsumerGroup(id string, topic string) (*pb.ConsumerGroup, error) {
	var group pb.ConsumerGroup
	err := f.MetaStore.View(func(tx *badger.Txn) error {
		v, err := tx.Get([]byte("ConsumerGroup-" + topic + "-" + id))
		if err != nil {
			return nil
		}
		var vbytes []byte
		v.Value(func(val []byte) error {
			vbytes = val
			return nil
		})
		err = util.DeserializeMessage(vbytes, &group)
		if err != nil {
			return fmt.Errorf("decoding issues with this %w", err)
		}
		return nil
	})
	if &group == nil {
		return nil, fmt.Errorf("consumer does not exist")
	} else if err != nil {
		return nil, fmt.Errorf("error with local store, %s", err)
	}
	return &group, nil
}

func (f *NodeState) Ack(request *pb.Ack) (interface{}, error) {
	group, err := f.getConsumerGroup(request.GroupId, request.Topic)
	if err != nil {
		return nil, err
	}
	partitionConsumer := make(map[uint64]*pb.Consumer)
	for _, consumer := range group.Consumers {
		partitionConsumer[consumer.Partition] = consumer
	}
	for partition, offset := range request.Offsets {
		partitionConsumer[partition].Offset = offset
	}
	err = f.MetaStore.Update(func(tx *badger.Txn) error {
		buf, err := util.SerializeMessage(group)
		if err != nil {
			return fmt.Errorf("decoding issues with this %w", err)
		}
		consumerId := []byte("ConsumerGroup-" + request.Topic + "-" + group.Id)
		err = tx.Set(consumerId, buf)
		if err != nil {
			f.Logger.Printf("error putting items in bucket, %s", err)
			return fmt.Errorf("error putting items in bucket, %w", err)
		}
		return nil
	})
	return &pb.AckConsumeResponse{}, nil
}
