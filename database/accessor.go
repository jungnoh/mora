package database

import (
	"github.com/jungnoh/mora/page"
)

type pageAccessor struct {
	db   *Database
	txId uint64
}

func (a *pageAccessor) Acquire(set page.CandleSet) (func(), error) {
	pageLock := a.db.lock.Memory.Get(set.UniqueKey())
	pageLock.Lock()
	return pageLock.Unlock, nil
}

func (a *pageAccessor) Get(set page.CandleSet) (*page.Page, error) {
	pg, err := a.db.loadPage(set, false)
	if pg.Header.LastTxId < a.txId {
		pg.Header.LastTxId = a.txId
	}
	return pg, err
}
