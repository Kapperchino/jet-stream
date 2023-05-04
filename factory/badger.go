package factory

import (
	"github.com/dgraph-io/badger/v3"
)

func NewBadger(logDir string, inMem bool) (*badger.DB, error) {
	var ops badger.Options
	if inMem {
		ops = badger.DefaultOptions("").WithInMemory(true)
	} else {
		ops = badger.DefaultOptions(logDir).WithInMemory(false)
	}
	return badger.Open(ops)
}
