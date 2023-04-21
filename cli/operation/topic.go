package operation

import (
	"fmt"
	"github.com/Kapperchino/jet-stream/application/proto/proto"
	"github.com/Kapperchino/jet-stream/client"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
)

type Topic struct {
	client *client.JetClient
}

func (t *Topic) createTopic(cCtx *cli.Context) (*proto.CreateTopicResponse, error) {
	topic := cCtx.String("topic")
	partitions := cCtx.Int("partitions")
	res, err := t.client.CreateTopic(topic, partitions)
	if err != nil {
		log.Fatal().Err(err)
	}
	return res, nil
}

func (t *Topic) createTopicAction(cCtx *cli.Context) error {
	res, _ := t.createTopic(cCtx)
	fmt.Printf("%v", res)
	return nil
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
				Action:  t.createTopicAction,
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
