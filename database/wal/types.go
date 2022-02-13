package wal

import (
	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/page"
)

type TxID uint64

type Log struct {
	Set  page.CandleSet
	Data common.Candle
}

type WriteAheadLog struct {
	Lock *util.LockSet
	TxID TxID
}

func (w WriteAheadLog) NextTxID() TxID {
	w.TxID++
	return w.TxID
}
