package concurrency

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type multiLevelLockSet struct {
	accessLock sync.Mutex
	manager    *LockManager
	rootLock   *MultiLevelLock
	locks      map[uint64]*MultiLevelLock
}

func newMultiLevelLockSet(manager *LockManager) *multiLevelLockSet {
	rootLock := NewMultiLevelLock(manager, nil, NewResourceNamePart("db"))
	locks := make(map[uint64]*MultiLevelLock)
	locks[rootLock.name.hashValue] = rootLock
	return &multiLevelLockSet{
		manager:  manager,
		rootLock: rootLock,
		locks:    locks,
	}
}

func (m *multiLevelLockSet) Get(resource ResourceName) *MultiLevelLock {
	m.accessLock.Lock()
	defer m.accessLock.Unlock()

	wantedKeys := make([]ResourceNamePart, len(resource.Parts)+1)
	copy(wantedKeys[1:], resource.Parts)
	wantedKeys[0] = NewResourceNamePart("db")

	var parentLock *MultiLevelLock = nil
	partIndex := len(wantedKeys)
	for ; partIndex >= 0; partIndex-- {
		resourceName := NewResourceName(wantedKeys[0:partIndex])
		if lock, ok := m.locks[resourceName.hashValue]; ok {
			parentLock = lock
			break
		}
	}
	for i := partIndex; i < len(wantedKeys); i++ {
		parentLock = NewMultiLevelLock(m.manager, parentLock, wantedKeys[i])
		m.locks[parentLock.name.hashValue] = parentLock
	}
	return parentLock
}

type DatabaseLock struct {
	manager *LockManager
	lockSet *multiLevelLockSet
}

func NewDatabaseLock() *DatabaseLock {
	manager := LockManager{
		txLocks: NewTransactionLockMap(),
		entries: make(map[uint64]*LockEntry),
	}
	return &DatabaseLock{
		manager: &manager,
		lockSet: newMultiLevelLockSet(&manager),
	}
}

func (d *DatabaseLock) EnsureLock(txId TransactionId, resource ResourceName, lockType LockType) error {
	if lockType == NoLock {
		return nil
	}
	if lockType != SLock && lockType != XLock {
		return errors.Errorf("unexpected lock type '%s'", lockType)
	}

	lock := d.lockSet.Get(resource)
	explicitLockType := lock.manager.LockType(txId, lock.name)
	effectiveLockType := lock.EffectiveLockType(txId)
	if effectiveLockType.CanSubsitute(lockType) {
		log.Debug().Uint64("txId", uint64(txId)).Stringer("resource", resource).Stringer("type", lockType).Msg("lock changes not necessary")
		return nil
	}

	if explicitLockType != NoLock {
		if err := lock.Esclate(txId); err != nil {
			return errors.Wrap(err, "failed to esclate")
		}
		lockTypeAfterEsclate := lock.LockType(txId)
		if lockTypeAfterEsclate != SLock && lockTypeAfterEsclate != XLock {
			panic(errors.Errorf("lock for '%s' esclated but not S/X", lock.name))
		}
		if lockTypeAfterEsclate == SLock && lockType == XLock {
			if err := lock.Promote(txId, XLock); err != nil {
				return errors.Wrap(err, "failed to promote S->X after esclation")
			}
		}
		return nil
	}

	if lockType == SLock {
		if err := d.acquireSIntent(txId, lock.parent); err != nil {
			return err
		}
		return lock.Acquire(txId, SLock)
	}
	if err := d.acquireXIntent(txId, lock.parent); err != nil {
		return err
	}
	return lock.Acquire(txId, XLock)
}

func (d *DatabaseLock) acquireSIntent(txId TransactionId, lock *MultiLevelLock) error {
	if lock == nil {
		return nil
	}
	lockType := lock.LockType(txId)
	if lockType == SLock || lockType == SIXLock || lockType == XLock {
		panic(errors.Errorf("unexpected ancestor lock '%s' while acquiring IS", lockType))
	}
	// Ancestors already have intent if IS or IX
	if lockType != NoLock {
		return nil
	}
	if lock.parent != nil {
		if err := d.acquireSIntent(txId, lock.parent); err != nil {
			return err
		}
	}
	return lock.Acquire(txId, ISLock)
}

func (d *DatabaseLock) acquireXIntent(txId TransactionId, lock *MultiLevelLock) error {
	if lock == nil {
		return nil
	}
	lockType := lock.LockType(txId)
	if lockType == XLock {
		panic(errors.Errorf("unexpected ancestor lock '%s' while acquiring IX", lockType))
	}
	// Ancestors already have intent if IX or SIX
	if lockType == IXLock || lockType == SIXLock {
		return nil
	}
	if lock.parent != nil {
		if err := d.acquireXIntent(txId, lock.parent); err != nil {
			return err
		}
	}
	if lockType == ISLock {
		return lock.Promote(txId, IXLock)
	}
	if lockType == SLock {
		return lock.Promote(txId, SIXLock)
	}
	return lock.Acquire(txId, IXLock)
}

func (d *DatabaseLock) Free(txId TransactionId) error {
	return d.manager.ReleaseAll(txId)
}

func (d *DatabaseLock) PrintAllLocks() {
	d.lockSet.rootLock.PrintAllLocks()
}
