package concurrency

import (
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// Hierarchy: Market -> Symbol -> Page

type MultiLevelLock struct {
	name                ResourceName
	lastNamePart        ResourceNamePart
	manager             *LockManager
	parent              *MultiLevelLock
	childrenLockCounter ChildSet
}

func traverseChildren(txId TransactionId, lock *MultiLevelLock, post *func(*MultiLevelLock) error) error {
	for _, child := range lock.childrenLockCounter.TransactionChildren(txId) {
		if err := traverseChildren(txId, child, post); err != nil {
			return err
		}
	}
	if post != nil {
		if err := (*post)(lock); err != nil {
			return err
		}
	}
	return nil
}

func releaseLockIfSIS(txId TransactionId, lock *MultiLevelLock) error {
	lockType := lock.manager.LockType(txId, lock.name)
	if lockType == SLock || lockType == ISLock {
		return lock.manager.Release(txId, lock.name)
	}
	return nil
}

func NewMultiLevelLock(manager *LockManager, parent *MultiLevelLock, key ResourceNamePart) *MultiLevelLock {
	lock := MultiLevelLock{
		manager:             manager,
		parent:              parent,
		childrenLockCounter: *NewTransactionRefCounter(),
	}
	if parent != nil && parent.manager != manager {
		panic("parent and child have different managers")
	}
	if parent != nil {
		lock.name = parent.name.CreateChild(key)
	} else {
		lock.name = NewResourceName([]ResourceNamePart{key})
	}
	return &lock
}

func (m *MultiLevelLock) EffectiveLockType(txId TransactionId) LockType {
	if lockType := m.LockType(txId); lockType != NoLock {
		return lockType
	}
	parentLock := m.parent
	for parentLock != nil {
		lockType := parentLock.manager.LockType(txId, parentLock.name)
		if lockType == NoLock {
			parentLock = parentLock.parent
			continue
		}
		if lockType == ISLock || lockType == IXLock {
			break
		}
		return lockType
	}
	return NoLock
}

func (m *MultiLevelLock) LockType(txId TransactionId) LockType {
	return m.manager.LockType(txId, m.name)
}

func (m *MultiLevelLock) Acquire(txId TransactionId, lockType LockType) error {
	log.Debug().Uint64("txId", uint64(txId)).Stringer("resource", m.name).Stringer("type", lockType).Msg("Acquire requested")
	if m.parent != nil {
		parentLockType := m.manager.LockType(txId, m.parent.name)
		if !LockTypesCanBeParent(parentLockType, lockType) {
			return errors.Errorf("incompatible lock type with parent (child %s, parent %s)", lockType, parentLockType)
		}
	}
	if err := m.manager.Acquire(txId, m.name, lockType); err != nil {
		return errors.Wrapf(err, "failed to lock (%s,%s,txId=%d)", m.name, lockType, txId)
	}
	if m.parent != nil {
		m.parent.childrenLockCounter.AddChild(m)
		m.parent.childrenLockCounter.AddReference(txId, m.lastNamePart)
	}
	return nil
}

func (m *MultiLevelLock) Release(txId TransactionId) error {
	log.Debug().Uint64("txId", uint64(txId)).Stringer("resource", m.name).Msg("Release requested")
	if count := m.childrenLockCounter.TransactionCount(txId); count > 0 {
		return errors.Errorf("trying to release when children are still locked (has %d)", count)
	}
	if err := m.manager.Release(txId, m.name); err != nil {
		return errors.Wrapf(err, "failed to release (%s,txId=%d)", m.name, txId)
	}
	if m.parent != nil {
		m.parent.childrenLockCounter.RemoveReference(txId, m.lastNamePart)
	}
	return nil
}

func (m *MultiLevelLock) Promote(txId TransactionId, newLockType LockType) error {
	log.Debug().Uint64("txId", uint64(txId)).Stringer("resource", m.name).Stringer("type", newLockType).Msg("Promotion requested")
	prevLockType := m.LockType(txId)
	if prevLockType == newLockType {
		return errors.New("lock type cannot be same")
	}
	if !LockTypesSubstitutable(prevLockType, newLockType) {
		return errors.Errorf("lock types not substitutable (trying to substitute %s to %s)", prevLockType, newLockType)
	}
	if m.parent != nil {
		parentLockType := m.manager.LockType(txId, m.parent.name)
		if !LockTypesCanBeParent(parentLockType, newLockType) {
			return errors.Errorf("incompatible lock type with parent (child %s, parent %s)", newLockType, parentLockType)
		}
	}
	if err := m.manager.Promote(txId, m.name, newLockType); err != nil {
		return errors.Wrapf(err, "failed to promote (%s,%s->%s,txId=%d)", m.name, prevLockType, newLockType, txId)
	}

	// All S/IS locks on descendents should be released if IS/IX -> SIX
	if (prevLockType == ISLock || prevLockType == IXLock) && newLockType == SIXLock {
		iterate := func(lock *MultiLevelLock) error {
			return releaseLockIfSIS(txId, lock)
		}
		if err := traverseChildren(txId, m, &iterate); err != nil {
			return errors.Wrap(err, "failed to release S/IS locks")
		}
	}

	return nil
}

func (m *MultiLevelLock) Esclate(txId TransactionId) error {
	log.Debug().Uint64("txId", uint64(txId)).Stringer("resource", m.name).Msg("Esclation requested")
	prevLockType := m.LockType(txId)
	if prevLockType == SLock || prevLockType == XLock {
		return nil
	}
	if prevLockType == NoLock {
		return errors.New("no lock held")
	}

	toRelease := make([]ResourceName, 0)
	traverseStep := func(lock *MultiLevelLock) error {
		if lock != m && lock.manager.LockType(txId, lock.name) != NoLock {
			toRelease = append(toRelease, lock.name)
		}
		return nil
	}
	traverseChildren(txId, m, &traverseStep)

	var releaseErr error = nil
	if m.LockType(txId) == ISLock {
		releaseErr = m.manager.AcquireThenRelease(txId, m.name, SLock, toRelease)
	} else {
		releaseErr = m.manager.AcquireThenRelease(txId, m.name, XLock, toRelease)
	}
	if releaseErr != nil {
		return errors.Wrap(releaseErr, "failed to switch locks")
	}

	m.childrenLockCounter.ClearReferences(txId)
	return nil
}
