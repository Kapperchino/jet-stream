package operation

import (
	"encoding/json"
	"github.com/urfave/cli/v2"
	"os"
)

type InitOperator struct{}

func (i *InitOperator) GetCommand() *cli.Command {
	return &cli.Command{
		Name:    "init",
		Aliases: []string{"i"},
		Usage:   "Initializes the cli",
		Action:  i.initialize,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "address",
				Usage:    "address of a node",
				Aliases:  []string{"a"},
				Required: true,
			},
		},
	}

}

func (*InitOperator) initialize(cCtx *cli.Context) error {
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
