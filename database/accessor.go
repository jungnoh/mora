package database

import (
	"github.com/jungnoh/mora/page"
)

type pageAccessor struct {
	db    *Database
	txId  uint64
	pages map[string]bool
}

func (a *pageAccessor) Acquire(set page.CandleSet) (func(), error) {
	key := set.UniqueKey()
	pageLock := a.db.lock.Memory.Get(key)
	pageLock.Lock()
	a.pages[key] = true
	a.db.Mem.Ref(key)
	return pageLock.Unlock, nil
}

func (a *pageAccessor) Get(set page.CandleSet) (*page.Page, error) {
	pg, err := a.db.loadPage(set, false)
	if pg.Header.LastTxId < a.txId {
		pg.Header.LastTxId = a.txId
	}
	return pg, err
}

func (a *pageAccessor) Free() {
	for key := range a.pages {
		a.db.Mem.FreeRef(key)
	}
}
