package operation

import (
	"fmt"
	"github.com/Kapperchino/jet-stream/client"
	"github.com/urfave/cli/v2"
)

type Consumer struct {
	client *client.JetClient
}

func (p *Consumer) process(cCtx *cli.Context) (any, error) {
	return nil, nil
}

func (p *Consumer) Serialize(cCtx *cli.Context) error {
	res, _ := p.process(cCtx)
	fmt.Printf("%v\n", res)
	return nil
}

func (p *Consumer) GetCommand() *cli.Command {
	return &cli.Command{
		Name:    "consumer",
		Aliases: []string{"c"},
		Usage:   "Consumer commands",
		Subcommands: []*cli.Command{
			{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "Topic",
						Usage:    "Topic to consume from",
						Aliases:  []string{"t"},
						Required: true,
					},
					&cli.StringFlag{
						Name:     "id",
						Usage:    "id for the consumer group",
						Required: true,
					},
					&cli.IntFlag{
						Name:    "batch-size",
						Aliases: []string{"s"},
						Usage:   "size of the batches to get",
						Value:   20,
					},
				},
				Name:    "consume",
				Aliases: []string{"con"},
				Usage:   "consume from a Topic",
				Action: func(cCtx *cli.Context) error {
					fmt.Println("new task template: ", cCtx.Args().First())
					return nil
				},
			},
			{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "Topic",
						Usage:    "Topic to consume from",
						Aliases:  []string{"t"},
						Required: true,
					},
				},
				Name:    "create",
				Aliases: []string{"cre"},
				Usage:   "create a consumer",
				Action: func(cCtx *cli.Context) error {
					fmt.Println("new task template: ", cCtx.Args().First())
					return nil
				},
			},
			{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "Topic",
						Usage:    "Topic of the consumers",
						Aliases:  []string{"t"},
						Required: true,
					},
				},
				Name:    "list",
				Aliases: []string{"l"},
				Usage:   "get a list of consumer",
				Action: func(cCtx *cli.Context) error {
					fmt.Println("new task template: ", cCtx.Args().First())
					return nil
				},
			},
		},
	}
}
