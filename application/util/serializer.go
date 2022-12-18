package util

import (
	config "github.com/Kapperchino/jet-application/config"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func SerializeMessage(m proto.Message) ([]byte, error) {
	if config.DEV_MODE {
		return protojson.Marshal(m)
	}
	return proto.Marshal(m)
}

func DeserializeMessage(b []byte, m proto.Message) error {
	if config.DEV_MODE {
		return protojson.Unmarshal(b, m)
	}
	return proto.Unmarshal(b, m)
}
