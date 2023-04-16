package main

import (
	"github.com/Kapperchino/jet-stream/cli/util"
	"github.com/rs/zerolog/log"
	"os"
)

func main() {
	app, err := util.InitCli()
	if err != nil {
		return
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal().Err(err)
	}
}
