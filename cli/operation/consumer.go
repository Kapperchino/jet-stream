package operation

import (
	"fmt"
	"github.com/Kapperchino/jet-stream/application/proto/proto"
	"github.com/Kapperchino/jet-stream/client"
	"github.com/Kapperchino/jet-stream/util"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
	"strconv"
)

type Consumer struct {
	client *client.JetClient
}

func (p *Consumer) consume(cCtx *cli.Context) ([]*proto.Message, error) {
	topic := cCtx.String("Topic")
	id := cCtx.String("id")
	msgs, err := p.client.ConsumeMessage(topic, id)
	if err != nil {
		log.Fatal().Err(err)
	}
	return msgs, nil
}

func (p *Consumer) createConsumer(cCtx *cli.Context) (*proto.CreateConsumerGroupResponse, error) {
	topic := cCtx.String("Topic")
	consumer, err := p.client.CreateConsumerGroup(topic)
	if err != nil {
		log.Fatal().Err(err)
	}
	return consumer, nil
}

func (p *Consumer) consumeAction(cCtx *cli.Context) error {
	res, _ := p.consume(cCtx)
	longest := 0
	for _, message := range res {
		longest = util.Max(longest, len(message.Key))
		longest = util.Max(longest, len(message.Payload))
	}
	longest += 5
	for _, message := range res {
		fmt.Printf("key: %-"+strconv.Itoa(longest)+"s", message.Key)
		fmt.Printf("val: %-"+strconv.Itoa(longest)+"s", message.Payload)
		fmt.Printf("partition: %-5d", message.Partition)
		fmt.Printf("offset: %-5v\n", message.Offset)
	}
	return nil
}

func (p *Consumer) createConsumerAction(cCtx *cli.Context) error {
	res, _ := p.createConsumer(cCtx)
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
				Action:  p.consumeAction,
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
				Action:  p.createConsumerAction,
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
