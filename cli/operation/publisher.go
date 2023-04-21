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

const CliDir = "/.config/jet-cli/"
const CliFile = "config.json"

type Publisher struct {
	client *client.JetClient
}

func (p *Publisher) process(cCtx *cli.Context) (*proto.PublishMessageResponse, error) {
	messageStr := cCtx.StringSlice("messages")
	topic := cCtx.String("Topic")
	arr := make([]*proto.KeyVal, 0)
	for i := 0; i < len(messageStr)-1; i += 2 {
		arr = append(arr, &proto.KeyVal{
			Key: []byte(messageStr[i]),
			Val: []byte(messageStr[i+1]),
		})
	}
	res, err := p.client.PublishMessage(arr, topic)
	if err != nil {
		log.Err(err).Msgf("Error when publishing")
		return nil, err
	}
	return res, nil
}

func (p *Publisher) publish(cCtx *cli.Context) error {
	res, _ := p.process(cCtx)
	longest := 0
	for _, message := range res.Messages {
		longest = util.Max(longest, len(message.Key))
		longest = util.Max(longest, len(message.Payload))
	}
	longest += 5
	for _, message := range res.Messages {
		fmt.Printf("key: %-"+strconv.Itoa(longest)+"s", message.Key)
		fmt.Printf("val: %-"+strconv.Itoa(longest)+"s", message.Payload)
		fmt.Printf("partition: %-5d", message.Partition)
		fmt.Printf("offset: %-5v\n", message.Offset)
	}
	return nil
}

func (p *Publisher) GetCommand() *cli.Command {
	return &cli.Command{
		Name:    "publisher",
		Aliases: []string{"p"},
		Usage:   "Publisher commands",
		Subcommands: []*cli.Command{
			{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "Topic",
						Usage:    "Topic to publish to",
						Aliases:  []string{"t"},
						Required: true,
					},
					&cli.StringSliceFlag{
						Name:    "messages",
						Usage:   "messages to publish",
						Aliases: []string{"m"},
					},
					&cli.StringSliceFlag{
						Name:      "file",
						Usage:     "file in the format of key,val to publish",
						Aliases:   []string{"f"},
						TakesFile: true,
					},
				},
				Name:    "publish",
				Aliases: []string{"pub"},
				Usage:   "publish to a Topic",
				Action:  p.publish,
			},
		},
	}
}
