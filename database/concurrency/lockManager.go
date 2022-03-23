package concurrency

import (
	"sync"

	errSlice "github.com/carlmjohnson/errors"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type LockManager struct {
	txLocks     TransactionLockMap
	entries     map[uint64]*LockEntry
	entriesLock sync.Mutex
}

func (l *LockManager) getResourceEntry(name ResourceName) *LockEntry {
	l.entriesLock.Lock()
	defer l.entriesLock.Unlock()

	if _, ok := l.entries[name.hashValue]; !ok {
		l.entries[name.hashValue] = NewLockEntry(name, &l.txLocks)
	}
	return l.entries[name.hashValue]
}

func (l *LockManager) Acquire(txId TransactionId, name ResourceName, lockType LockType) error {
	log.Debug().Uint64("txId", uint64(txId)).Stringer("resource", name).Stringer("type", lockType).Msg("Acquire")
	wantedLock := Lock{Name: name, Transaction: txId, Type: lockType}
	ack := make(chan bool)
	resource := l.getResourceEntry(name)

	if l.txLocks.Contains(wantedLock) {
		return errors.Errorf("'%s' lock already held by this transaction", lockType)
	}
	go resource.AddToQueue(lockRequest{Lock: wantedLock, Ack: ack}, false)
	<-ack
	l.txLocks.AddLock(txId, name, lockType)
	return nil
}

func (l *LockManager) AcquireThenRelease(txId TransactionId, name ResourceName, lockType LockType, releases []ResourceName) error {
	log.Debug().Uint64("txId", uint64(txId)).Stringer("resource", name).Stringer("type", lockType).Msg("AcquireThenRelease")
	for _, release := range releases {
		if !l.txLocks.ContainsResource(txId, release) {
			return errors.Errorf("tx does not hold lock on '%s'", release)
		}
	}

	wantedLock := Lock{Transaction: txId, Name: name, Type: lockType}
	if l.txLocks.Contains(wantedLock) {
		return errors.New("tx already has wanted lock")
	}
	resource := l.getResourceEntry(name)
	ack := make(chan bool)
	go resource.AddToQueue(lockRequest{Lock: wantedLock, Ack: ack}, false)
	<-ack
	l.txLocks.AddLock(txId, name, lockType)

	for _, release := range releases {
		if err := l.Release(txId, release); err != nil {
			return errors.Wrapf(err, "error releasing (%d,%s)", txId, release)
		}
	}
	return nil
}

func (l *LockManager) Promote(txId TransactionId, name ResourceName, newLockType LockType) error {
	log.Debug().Uint64("txId", uint64(txId)).Stringer("resource", name).Stringer("type", newLockType).Msg("Promote")
	if !l.txLocks.ContainsResource(txId, name) {
		return errors.New("Transaction does not have lock on this resource")
	}
	resourceEntry := l.getResourceEntry(name)
	existingType := resourceEntry.TransactionLockType(txId)
	if existingType == newLockType {
		return errors.Errorf("Cannot promote equivalent lock (%s)", newLockType)
	}
	if !newLockType.CanSubsitute(existingType) {
		return errors.Errorf("Cannot promote lock '%s' to '%s'", existingType, newLockType)
	}

	ack := make(chan bool)
	wantedLock := Lock{Name: name, Transaction: txId, Type: newLockType}
	go resourceEntry.AddToQueue(lockRequest{Lock: wantedLock, Ack: ack}, false)
	<-ack
	return nil
}

func (l *LockManager) Release(txId TransactionId, name ResourceName) error {
	log.Debug().Uint64("txId", uint64(txId)).Stringer("resource", name).Msg("Release")
	if l.txLocks.ContainsResource(txId, name) {
		return errors.New("Transaction does not have lock on this resource")
	}
	resource := l.getResourceEntry(name)
	resource.Release(txId)
	l.txLocks.DeleteResource(txId, name)
	return nil
}

func (l *LockManager) ReleaseAll(txId TransactionId) error {
	log.Debug().Uint64("txId", uint64(txId)).Msg("ReleaseAll")
	var errs errSlice.Slice
	for _, resource := range l.txLocks.AllResources(txId) {
		errs.Push(l.Release(txId, resource))
	}
	return errs.Merge()
}

func (l *LockManager) LockType(txId TransactionId, name ResourceName) LockType {
	entry := l.getResourceEntry(name)
	return entry.TransactionLockType(txId)
}
