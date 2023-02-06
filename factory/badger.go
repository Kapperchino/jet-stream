package factory

import (
	"github.com/dgraph-io/badger/v3"
)

func NewBadger(logDir string) (*badger.DB, error) {
	ops := badger.DefaultOptions(logDir)
	ops.InMemory = false
	return badger.Open(ops)
}
