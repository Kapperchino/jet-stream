package fsm

import (
	"github.com/Kapperchino/jet-stream/util"
)

func makePrefix(topic string, partition uint64) []byte {
	prefix := append([]byte(topic+"-"), util.ULongToBytes(partition)...)
	return prefix
}

func makeSeqKey(topic string, partition uint64) []byte {
	prefix := append([]byte(topic+"/"), util.ULongToBytes(partition)...)
	return prefix
}

func makeKey(topic string, partition uint64, offset uint64) []byte {
	prefix := makePrefix(topic, partition)
	msg := append(prefix, util.ULongToBytes(offset)...)
	return msg
}
