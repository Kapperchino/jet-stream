package factory

import (
	"bytes"
	"errors"
	"github.com/Kapperchino/jet-stream/util"
	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
)

type BadgerLogStore struct {
	LogStore *badger.DB
}

func (b BadgerLogStore) Set(key []byte, val []byte) error {
	err := b.LogStore.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, val)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (b BadgerLogStore) Get(key []byte) ([]byte, error) {
	var res []byte
	err := b.LogStore.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			res = val
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return []byte{}, nil
		}
		return nil, err
	}
	return res, nil
}

func (b BadgerLogStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, util.ULongToBytes(val))
}

func (b BadgerLogStore) GetUint64(key []byte) (uint64, error) {
	buf, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}
	return util.BytesToULong(buf), nil
}

func (b BadgerLogStore) FirstIndex() (uint64, error) {
	var num uint64 = 0
	err := b.LogStore.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Rewind()
		if it.Valid() {
			it.Next()
			num = util.BytesToULong(it.Item().Key())
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return num, nil
}

func (b BadgerLogStore) LastIndex() (uint64, error) {
	var num uint64 = 0
	err := b.LogStore.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Rewind()
		if it.Valid() {
			it.Next()
			num = util.BytesToULong(it.Item().Key())
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return num, nil
}

func (b BadgerLogStore) GetLog(index uint64, log *raft.Log) error {
	err := b.LogStore.View(func(txn *badger.Txn) error {
		item, err := txn.Get(util.ULongToBytes(index))
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			return decodeMsgPack(val, log)
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (b BadgerLogStore) StoreLog(log *raft.Log) error {
	err := b.LogStore.Update(func(txn *badger.Txn) error {
		buf, err := encodeMsgPack(log)
		if err != nil {
			return err
		}
		err = txn.Set(util.ULongToBytes(log.Index), buf.Bytes())
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (b BadgerLogStore) StoreLogs(logs []*raft.Log) error {
	err := b.LogStore.Update(func(txn *badger.Txn) error {
		for _, log := range logs {
			buf, err := encodeMsgPack(log)
			if err != nil {
				return err
			}
			err = txn.Set(util.ULongToBytes(log.Index), buf.Bytes())
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (b BadgerLogStore) DeleteRange(min, max uint64) error {
	err := b.LogStore.Update(func(txn *badger.Txn) error {
		for i := min; i <= max; i++ {
			err := txn.Delete(util.ULongToBytes(i))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}
