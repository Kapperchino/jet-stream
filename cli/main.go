package main

import (
	"github.com/Kapperchino/jet-stream/cli/operation"
	"github.com/rs/zerolog/log"
	"os"
)

func main() {
	jet, err := operation.NewJetCli()
	if err != nil {
		log.Fatal().Err(err)
	}
	app, err := jet.InitCli()
	if err != nil {
		log.Fatal().Err(err)
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal().Err(err)
	}
}
