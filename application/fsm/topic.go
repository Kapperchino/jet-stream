package fsm

import (
	"fmt"
	pb "github.com/Kapperchino/jet-application/proto/proto"
	"github.com/Kapperchino/jet/util"
	"github.com/dgraph-io/badger/v3"
)

func (f *NodeState) CreateTopic(req *pb.CreateTopic) (interface{}, error) {
	curTopic, err := f.getTopic(req.GetTopic())
	//already exists
	if err == nil && curTopic != nil {
		return new(pb.CreateTopicResponse), nil
	}
	newTopic := pb.Topic{
		Name:       req.GetTopic(),
		Partitions: map[uint64]*pb.Partition{},
	}
	for _, i := range req.GetPartitions() {
		newTopic.Partitions[i] = f.CreatePartition(i, req.GetTopic())
	}
	//seralize Topic and put in db
	res, err := util.SerializeMessage(&newTopic)
	if err != nil {
		return nil, fmt.Errorf("error encoding Topic")
	}
	err = f.MetaStore.Update(func(tx *badger.Txn) error {
		err = tx.Set([]byte("Topic-"+newTopic.Name), res)
		if err != nil {
			return err
		}
		f.Logger.Printf("Created Topic %s with partitions %v", req.GetTopic(), req.GetPartitions())
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error saving Topic")
	}
	return new(pb.CreateTopicResponse), nil
}

func (f *NodeState) getTopic(topicName string) (*pb.Topic, error) {
	var curTopic pb.Topic
	err := f.MetaStore.View(func(tx *badger.Txn) error {
		v, err := tx.Get([]byte("Topic-" + topicName))
		if err != nil {
			return err
		}
		var valBytes []byte
		v.Value(func(val []byte) error {
			valBytes = val
			return nil
		})
		err = util.DeserializeMessage(valBytes, &curTopic)
		if err != nil {
			f.Logger.Err(err).Msgf("error deserializing topic")
			return err
		}
		return nil
	})
	if &curTopic == nil {
		return nil, fmt.Errorf("topic does not exist, %w", err)
	} else if err != nil {
		return nil, fmt.Errorf("error with local store, %s", err)
	}
	return &curTopic, nil
}

func (f *NodeState) getTopics() (map[string]*pb.Topic, error) {
	topics := map[string]*pb.Topic{}
	err := f.MetaStore.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := tx.NewIterator(opts)
		defer it.Close()
		prefix := []byte("Topic-")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			//now we need to seek until the message is found
			item := it.Item()
			err := item.Value(func(v []byte) error {
				var topic pb.Topic
				err := util.DeserializeMessage(v, &topic)
				if err != nil {
					return err
				}
				topics[topic.Name] = &topic
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
	return topics, nil
}

func (f *NodeState) CreatePartition(partitionNum uint64, topic string) *pb.Partition {
	res := pb.Partition{
		Num:    partitionNum,
		Topic:  topic,
		Offset: 0,
	}
	return &res
}
