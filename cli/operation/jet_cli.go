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

func StartUpCli() (*JetCli, error) {
	dir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	_, err = os.Stat(dir + CliDir + CliFile)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		log.Warn().Msgf("Config does not exist, please run init")
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
	return &JetCli{client: jetClient}, nil
}

func Initialize(cCtx *cli.Context) error {
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
	err = os.Mkdir(dir+CliDir, 0755)
	if err != nil {
		return err
	}
	err = os.WriteFile(dir+CliDir+CliFile, buf, 0644)
	if err != nil {
		return err
	}
	return nil
}
