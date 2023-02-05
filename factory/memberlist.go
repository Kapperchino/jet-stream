package factory

import (
	"fmt"
	"github.com/Kapperchino/jet/util"
	"github.com/hashicorp/memberlist"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"net"
	"os"
	"strconv"
	"time"
)

func NewMemberList(config *memberlist.Config, rootNode string) *memberlist.Memberlist {
	list, err := memberlist.Create(config)
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}
	if len(rootNode) != 0 {
		// Join an existing cluster by specifying at least one known member.
		_, err := list.Join([]string{rootNode})
		if err != nil {
			panic("Failed to join cluster: " + err.Error())
		}
	}
	// Ask for members of the cluster
	for _, member := range list.Members() {
		log.Printf("Member: %s %s", member.Name, member.Addr)
	}
	return list
}

func MakeConfig(nodeName string, shardName string, gossipAddress string, eventDelegate memberlist.EventDelegate, delegate memberlist.Delegate) *memberlist.Config {
	host, port, _ := net.SplitHostPort(gossipAddress)
	portInt, _ := strconv.Atoi(port)
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006/01/02 15:04:05"}
	output.FormatLevel = func(i interface{}) string {
		return fmt.Sprintf("[%s]", nodeName)
	}
	output.FormatFieldName = func(i interface{}) string {
		return fmt.Sprintf("%s:", i)
	}
	stdLogger := util.NewStdLoggerWithOutput(output)
	return &memberlist.Config{
		Name:                    shardName,
		BindAddr:                host,
		BindPort:                portInt,
		AdvertiseAddr:           "127.0.0.1",
		AdvertisePort:           portInt,
		ProtocolVersion:         memberlist.ProtocolVersion2Compatible,
		TCPTimeout:              10 * time.Second,       // Timeout after 10 seconds
		IndirectChecks:          3,                      // Use 3 nodes for the indirect ping
		RetransmitMult:          4,                      // Retransmit a message 4 * log(N+1) nodes
		SuspicionMult:           4,                      // Suspect a node for 4 * log(N+1) * Interval
		SuspicionMaxTimeoutMult: 6,                      // For 10k nodes this will give a max timeout of 120 seconds
		PushPullInterval:        30 * time.Second,       // Low frequency
		ProbeTimeout:            500 * time.Millisecond, // Reasonable RTT time for LAN
		ProbeInterval:           1 * time.Second,        // Failure check every second
		DisableTcpPings:         false,                  // TCP pings are safe, even with mixed versions
		AwarenessMaxMultiplier:  8,                      // Probe interval backs off to 8 seconds
		GossipNodes:             3,                      // Gossip to 3 nodes
		GossipInterval:          200 * time.Millisecond, // Gossip more rapidly
		GossipToTheDeadTime:     30 * time.Second,       // Same as push/pull
		GossipVerifyIncoming:    true,
		GossipVerifyOutgoing:    true,
		Logger:                  stdLogger,

		EnableCompression: true, // Enable compression by default

		SecretKey: nil,
		Keyring:   nil,

		DNSConfigPath: "/etc/resolv.conf",

		HandoffQueueDepth: 1024,
		UDPBufferSize:     1400,
		CIDRsAllowed:      nil, // same as allow all

		QueueCheckInterval: 30 * time.Second,
		Events:             eventDelegate,
		Delegate:           delegate,
	}
}
