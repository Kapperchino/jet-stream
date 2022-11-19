package main

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func serializeMessage(m proto.Message) ([]byte, error) {
	if DEV_MODE {
		return protojson.Marshal(m)
	}
	return proto.Marshal(m)
}

func deserializeMessage(b []byte, m proto.Message) error {
	if DEV_MODE {
		return protojson.Unmarshal(b, m)
	}
	return proto.Unmarshal(b, m)
}
