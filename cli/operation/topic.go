package operation

import (
	"fmt"
	"github.com/urfave/cli/v2"
)

type Topic struct {
}

func (t *Topic) process(cCtx *cli.Context) (any, error) {
	//TODO implement me
	panic("implement me")
}

func (t *Topic) Serialize(cCtx *cli.Context) error {
	//TODO implement me
	panic("implement me")
}

func (t *Topic) GetCommand() *cli.Command {
	return &cli.Command{
		Name:    "Topic",
		Aliases: []string{"t"},
		Usage:   "Topic commands",
		Subcommands: []*cli.Command{
			{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "name",
						Usage:    "name of the Topic",
						Aliases:  []string{"n"},
						Required: true,
					},
					&cli.IntFlag{
						Name:     "partitions",
						Usage:    "number of partitions in the Topic",
						Aliases:  []string{"p"},
						Required: true,
					},
				},
				Name:    "create",
				Aliases: []string{"cre"},
				Usage:   "creates a new Topic",
				Action: func(cCtx *cli.Context) error {
					fmt.Println("new task template: ", cCtx.Args().First())
					return nil
				},
			},
			{
				Name:    "list",
				Aliases: []string{"l"},
				Usage:   "list all the topics",
				Action: func(cCtx *cli.Context) error {
					fmt.Println("new task template: ", cCtx.Args().First())
					return nil
				},
			},
			{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "name",
						Usage:    "name of the Topic",
						Aliases:  []string{"n"},
						Required: true,
					},
				},
				Name:    "delete",
				Aliases: []string{"del"},
				Usage:   "delete a Topic",
				Action: func(cCtx *cli.Context) error {
					fmt.Println("removed task template: ", cCtx.Args().First())
					return nil
				},
			},
		},
	}
}
