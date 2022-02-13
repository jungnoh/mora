package wal

import (
	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/page"
)

func (w WriteAheadLog) Write(txID TxID, set page.CandleSet, candles []common.Candle) error {
	return nil
}

// func (w WriteAheadLog)

type WriteAheadLogBuilder struct {
}
