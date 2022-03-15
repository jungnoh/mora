package concurrency

import (
	"sync"

	"github.com/pkg/errors"
)

type LockManager struct {
	txLocks     TransactionLockMap
	entries     map[ResourceName]*LockEntry
	entriesLock sync.Mutex
}

func (l *LockManager) getResourceEntry(name ResourceName) *LockEntry {
	l.entriesLock.Lock()
	defer l.entriesLock.Unlock()

	if _, ok := l.entries[name]; !ok {
		l.entries[name] = NewLockEntry(name, &l.txLocks)
	}
	return l.entries[name]
}

func (l *LockManager) Acquire(txId TransactionId, name ResourceName, lockType LockType) error {
	wantedLock := Lock{Name: name, Transaction: txId, Type: lockType}
	ack := make(chan bool)
	resource := l.getResourceEntry(name)

	if l.txLocks.Contains(wantedLock) {
		return errors.Errorf("'%s' lock already held by this transaction", lockType)
	}
	go resource.AddToQueue(lockRequest{Lock: wantedLock, Ack: ack}, false)
	<-ack
	return nil
}

func (l *LockManager) Release(txId TransactionId, name ResourceName) error {
	// TODO: Update LockManager map state
	if l.txLocks.ContainsResource(txId, name) {
		return errors.New("Transaction does not have lock on this resource")
	}
	resource := l.getResourceEntry(name)
	resource.Release(txId)
	return nil
}

func (l *LockManager) Promote(txId TransactionId, name ResourceName, newLockType LockType) error {
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
