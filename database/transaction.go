package database

import (
	"sort"

	errSlice "github.com/carlmjohnson/errors"
	"github.com/jungnoh/mora/database/command"
	"github.com/jungnoh/mora/database/concurrency"
	"github.com/jungnoh/mora/database/storage"
	"github.com/pkg/errors"
)

type TransactionContext struct {
	accessor *storage.StorageAccessor
	dbLock   *concurrency.DatabaseLock
	txId     concurrency.TransactionId
	finished bool
}

func NewTransactionContext(accessor *storage.StorageAccessor, dbLock *concurrency.DatabaseLock) TransactionContext {
	ctx := TransactionContext{
		accessor: accessor,
		dbLock:   dbLock,
	}
	return ctx
}

func (t *TransactionContext) Start() error {
	txId, err := t.accessor.Start()
	t.txId = concurrency.TransactionId(txId)
	return err
}

func (t *TransactionContext) Execute(cmd command.CommandContent) (interface{}, error) {
	plan := cmd.Plan()
	sort.Sort(plan.NeededLocks)
	for _, lock := range plan.NeededLocks {
		lockType := concurrency.SLock
		if lock.Exclusive {
			lockType = concurrency.XLock
		}
		if err := t.dbLock.EnsureLock(t.txId, lock.Lock, lockType); err != nil {
			return struct{}{}, errors.Wrapf(err, "failed to lock")
		}
	}
	result, err := t.accessor.Execute(cmd)
	if err != nil {
		return result, errors.Wrapf(err, "failed to execute command '%s'", cmd.String())
	}
	return result, nil
}

func (t *TransactionContext) Commit() error {
	t.finished = true
	var errs errSlice.Slice
	errs.Push(t.accessor.Commit())
	errs.Push(t.dbLock.Free(t.txId))
	return errs.Merge()
}

func (t *TransactionContext) Rollback() error {
	t.finished = true
	t.accessor.Rollback()
	return t.dbLock.Free(t.txId)
}

func (t *TransactionContext) RollbackIfActive() {
	if t.finished {
		return
	}
	t.Rollback()
}
