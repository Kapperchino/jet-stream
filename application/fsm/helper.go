package fsm

import (
	"github.com/Kapperchino/jet/util"
	"strconv"
)

func makePrefix(topic string, partition uint64) []byte {
	prefix := topic + "-" + strconv.FormatUint(partition, 10)
	return []byte(prefix)
}

func makeKey(topic string, partition uint64, offset uint64) []byte {
	prefix := makePrefix(topic, partition)
	msg := append(prefix, util.ULongToBytes(offset)...)
	return msg
}
