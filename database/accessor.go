package database

import "github.com/jungnoh/mora/page"

type pageAccessor struct {
	db *Database
}

func (a *pageAccessor) Acquire(set page.CandleSet) (func(), error) {
	pageLock := a.db.lock.Memory.Get(set.UniqueKey())
	pageLock.Lock()
	return pageLock.Unlock, nil
}

func (a *pageAccessor) Get(set page.CandleSet) (*page.Page, error) {
	return a.db.loadPage(set, false)
}
