package wal

import (
	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/page"
)

func (w WriteAheadLog) Write(txID uint64, set page.CandleSet, candles []common.Candle) error {
	return nil
}
