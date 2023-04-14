package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Kapperchino/jet-stream/application/proto/proto"
	"github.com/Kapperchino/jet-stream/client"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
	"os"
)

const CLI_DIR = "/.config/jet-cli/"
const CLI_FILE = "config.json"

type JetCli struct {
	client *client.JetClient
}

type CliMeta struct {
	Address string
}

func startUpCli() (*JetCli, error) {
	dir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	_, err = os.Stat(dir + CLI_DIR + CLI_FILE)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		log.Warn().Msgf("Config does not exist, please run init")
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	buf, err := os.ReadFile(dir + CLI_DIR + CLI_FILE)
	if err != nil {
		return nil, err
	}
	var meta CliMeta
	err = json.Unmarshal(buf, &meta)
	if err != nil {
		return nil, err
	}
	jetClient, err := client.New(meta.Address)
	return &JetCli{client: jetClient}, nil
}

func initialize(cCtx *cli.Context) error {
	dir, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	address := cCtx.String("address")
	meta := CliMeta{
		Address: address,
	}
	buf, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	err = os.Mkdir(dir+CLI_DIR, 0755)
	if err != nil {
		return err
	}
	err = os.WriteFile(dir+CLI_DIR+CLI_FILE, buf, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (c *JetCli) publish(cCtx *cli.Context) error {
	messageStr := cCtx.StringSlice("messages")
	topic := cCtx.String("topic")
	arr := make([]*proto.KeyVal, 0)
	for i := 0; i < len(messageStr)-1; i += 2 {
		arr = append(arr, &proto.KeyVal{
			Key: []byte(messageStr[i]),
			Val: []byte(messageStr[i+1]),
		})
	}
	res, err := c.client.PublishMessage(arr, topic)
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", res)
	return nil
}
