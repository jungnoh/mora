package concurrency

import (
	"sync"
)

func NewLockEntry(name ResourceName, txLockMap *TransactionLockMap) *LockEntry {
	return &LockEntry{
		name:      name,
		locks:     make(map[TransactionId]Lock, 0),
		queue:     newLockQueue(),
		txLockMap: txLockMap,
	}
}

type LockEntry struct {
	accessLock sync.Mutex
	name       ResourceName
	locks      map[TransactionId]Lock
	queue      lockQueue
	txLockMap  *TransactionLockMap
}

func (l *LockEntry) TransactionLockType(txId TransactionId) LockType {
	l.accessLock.Lock()
	defer l.accessLock.Unlock()
	lock, ok := l.locks[txId]
	if !ok {
		return NoLock
	}
	return lock.Type
}

func (l *LockEntry) Release(txId TransactionId) {
	l.accessLock.Lock()
	defer l.accessLock.Unlock()

	delete(l.locks, txId)
	l.txLockMap.DeleteResource(txId, l.name)
	l.processQueue()
}

func (l *LockEntry) LockCompatible(lockType LockType, txId TransactionId) bool {
	l.accessLock.Lock()
	defer l.accessLock.Unlock()

	return l.lockCompatible(lockType, txId)
}

func (l *LockEntry) AddToQueue(request lockRequest, front bool) {
	l.accessLock.Lock()
	defer l.accessLock.Unlock()

	if !l.queue.HasNext() && l.lockCompatible(request.Lock.Type, request.Lock.Transaction) {
		l.grantLock(request.Lock)
		request.Ack <- true
		return
	}
	if front {
		l.queue.PushFront(request)
	} else {
		l.queue.PushEnd(request)
	}
}

func (l *LockEntry) processQueue() {
	popped := l.queue.PopMatching(func(item *lockRequest) bool {
		return l.lockCompatible(item.Lock.Type, item.Lock.Transaction)
	})
	if popped == nil {
		return
	}
	l.grantLock(popped.Lock)
	popped.Ack <- true
}

func (l *LockEntry) grantLock(lock Lock) {
	l.locks[lock.Transaction] = lock
}

func (l *LockEntry) lockCompatible(lockType LockType, txId TransactionId) bool {
	for _, lock := range l.locks {
		if !lockType.Compatible(lock.Type) {
			if lock.Transaction == txId {
				continue
			}
			return false
		}
	}
	return true
}
