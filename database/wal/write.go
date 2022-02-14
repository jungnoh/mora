package wal

import (
	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/page"
)

type WalWriter interface {
	Insert(page.CandleSet, []common.TimestampCandle) error
	Commit() error
}
