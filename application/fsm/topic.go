package fsm

import (
	"bytes"
	"encoding/gob"
	"fmt"
	pb "github.com/Kapperchino/jet-application/proto"
	"github.com/dgraph-io/badger/v3"
	"github.com/spaolacci/murmur3"
	"log"
	"strconv"
)

type Topic struct {
	Name       string
	Partitions []Partition
}

type Partition struct {
	Num    uint64
	Topic  string
	Offset uint64
}

func (f *NodeState) CreateTopic(req *pb.CreateTopic) (interface{}, error) {
	curTopic, err := f.getTopic(req.GetTopic())
	//already exists
	if err == nil && curTopic != nil {
		return new(pb.CreateTopicResponse), nil
	}
	newTopic := Topic{
		Name:       req.GetTopic(),
		Partitions: []Partition{},
	}
	for i := uint64(0); i < req.GetPartitions(); i++ {
		newTopic.Partitions = append(newTopic.Partitions, f.CreatePartition(i, req.GetTopic()))
	}
	//seralize Topic and put in db
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(newTopic)
	if err != nil {
		return nil, fmt.Errorf("error encoding Topic")
	}
	err = f.MetaStore.Update(func(tx *badger.Txn) error {
		err = tx.Set([]byte(newTopic.Name), buf.Bytes())
		if err != nil {
			return err
		}
		log.Printf("Created Topic %s", req.GetTopic())
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error saving Topic")
	}
	return new(pb.CreateTopicResponse), nil
}

func (f *NodeState) getTopic(topicName string) (*Topic, error) {
	var curTopic *Topic
	err := f.MetaStore.View(func(tx *badger.Txn) error {
		v, err := tx.Get([]byte(topicName))
		if err != nil {
			return nil
		}
		var valBytes []byte
		v.Value(func(val []byte) error {
			valBytes = val
			return nil
		})
		buf := bytes.NewBuffer(valBytes)
		dec := gob.NewDecoder(buf)
		if err := dec.Decode(&curTopic); err != nil {
			log.Fatal(err)
			return err
		}
		hasher := murmur3.New64()
		var partitionHashed []string
		for i := 0; i < len(curTopic.Partitions); i++ {
			_, _ = hasher.Write(make([]byte, i))
			partitionHashed = append(partitionHashed, strconv.FormatUint(hasher.Sum64(), 2))
			hasher.Reset()
		}
		return nil
	})
	if curTopic == nil {
		return nil, fmt.Errorf("topic does not exist, %w", err)
	} else if err != nil {
		return nil, fmt.Errorf("error with local store, %s", err)
	}
	return curTopic, nil
}

func (f *NodeState) CreatePartition(partitionNum uint64, topic string) Partition {
	res := Partition{
		Num:    partitionNum,
		Topic:  topic,
		Offset: 1,
	}
	return res
}
