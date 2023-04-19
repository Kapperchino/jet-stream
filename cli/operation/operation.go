package operation

import "github.com/urfave/cli/v2"

type Operation interface {
	GetCommand() *cli.Command
}
