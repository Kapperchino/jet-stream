package operation

import "github.com/urfave/cli/v2"

type Operation interface {
	process(cCtx *cli.Context) (any, error)
	Serialize(cCtx *cli.Context) error
}
