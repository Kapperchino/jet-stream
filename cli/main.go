package main

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
	"os"
)

func main() {
	app := &cli.App{
		EnableBashCompletion: true,
		Usage:                "Cli client for jet-stream, helps with managing and testing the cluster",
		Commands: []*cli.Command{
			{
				Name:    "publisher",
				Aliases: []string{"p"},
				Usage:   "Publisher commands",
				Action: func(cCtx *cli.Context) error {
					fmt.Println("added task: ", cCtx.Args().First())
					return nil
				},
				Subcommands: []*cli.Command{
					{
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "topic",
								Usage:    "topic to publish to",
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
						Usage:   "publish to a topic",
						Action: func(cCtx *cli.Context) error {
							fmt.Println("new task template: ", cCtx.Args().First())
							return nil
						},
					},
				},
			},
			{
				Name:    "consumer",
				Aliases: []string{"c"},
				Usage:   "Consumer commands",
				Action: func(cCtx *cli.Context) error {
					fmt.Println("completed task: ", cCtx.Args().First())
					return nil
				},
				Subcommands: []*cli.Command{
					{
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "topic",
								Usage:    "topic to consume from",
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
						Usage:   "consume from a topic",
						Action: func(cCtx *cli.Context) error {
							fmt.Println("new task template: ", cCtx.Args().First())
							return nil
						},
					},
					{
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "topic",
								Usage:    "topic to consume from",
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
								Name:     "topic",
								Usage:    "topic of the consumers",
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
			},
			{
				Name:    "topic",
				Aliases: []string{"t"},
				Usage:   "Topic commands",
				Subcommands: []*cli.Command{
					{
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "name",
								Usage:    "name of the topic",
								Aliases:  []string{"n"},
								Required: true,
							},
							&cli.IntFlag{
								Name:     "partitions",
								Usage:    "number of partitions in the topic",
								Aliases:  []string{"p"},
								Required: true,
							},
						},
						Name:    "create",
						Aliases: []string{"cre"},
						Usage:   "creates a new topic",
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
								Usage:    "name of the topic",
								Aliases:  []string{"n"},
								Required: true,
							},
						},
						Name:    "delete",
						Aliases: []string{"del"},
						Usage:   "delete a topic",
						Action: func(cCtx *cli.Context) error {
							fmt.Println("removed task template: ", cCtx.Args().First())
							return nil
						},
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal().Err(err)
	}
}
