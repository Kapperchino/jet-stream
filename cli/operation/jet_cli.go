package operation

import (
	"encoding/json"
	"errors"
	"github.com/Kapperchino/jet-stream/client"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
	"os"
)

type JetCli struct {
	client     *client.JetClient
	operations []Operation
}

type CliMeta struct {
	Address string
}

func NewJetCli() (*JetCli, error) {
	dir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	_, err = os.Stat(dir + CliDir + CliFile)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		log.Warn().Msgf("Config does not exist, please run InitOperator")
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	buf, err := os.ReadFile(dir + CliDir + CliFile)
	if err != nil {
		return nil, err
	}
	var meta CliMeta
	err = json.Unmarshal(buf, &meta)
	if err != nil {
		return nil, err
	}
	jetClient, err := client.New(meta.Address)

	operators := []Operation{&Publisher{client: jetClient}, &Consumer{client: jetClient}, &InitOperator{}}
	return &JetCli{client: jetClient, operations: operators}, nil
}

func (j JetCli) InitCli() (*cli.App, error) {
	commands := make([]*cli.Command, 0)
	for _, operation := range j.operations {
		commands = append(commands, operation.GetCommand())
	}
	app := &cli.App{
		EnableBashCompletion: true,
		Usage:                "JetCli client for jet-stream, helps with managing and testing the cluster",
		Commands:             commands,
	}
	return app, nil
}
