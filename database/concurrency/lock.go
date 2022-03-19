package concurrency

import "sync"

type Lock struct {
	Name        ResourceName
	Transaction TransactionId
	Type        LockType
}

type LockList []Lock

func (l LockList) Contains(lock Lock) bool {
	for _, v := range l {
		if v.Name.hashValue == lock.Name.hashValue && v.Transaction == lock.Transaction && v.Type == lock.Type {
			return true
		}
	}
	return false
}

type TransactionLockMap struct {
	data map[TransactionId]map[uint64][]LockType
	lock sync.Mutex
}

func (m *TransactionLockMap) LockTypes(txId TransactionId, name ResourceName) []LockType {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.data[txId]; !ok {
		m.data[txId] = make(map[uint64][]LockType)
		m.data[txId][name.hashValue] = make([]LockType, 0)
		return []LockType{}
	}
	if _, ok := m.data[txId][name.hashValue]; !ok {
		m.data[txId][name.hashValue] = make([]LockType, 0)
		return []LockType{}
	}
	return m.data[txId][name.hashValue]
}

func (m *TransactionLockMap) Contains(lock Lock) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.data[lock.Transaction]; !ok {
		m.data[lock.Transaction] = make(map[uint64][]LockType)
		m.data[lock.Transaction][lock.Name.hashValue] = make([]LockType, 0)
		return false
	}
	if _, ok := m.data[lock.Transaction][lock.Name.hashValue]; !ok {
		m.data[lock.Transaction][lock.Name.hashValue] = make([]LockType, 0)
		return false
	}
	for _, v := range m.data[lock.Transaction][lock.Name.hashValue] {
		if v == lock.Type {
			return true
		}
	}
	return false
}

func (m *TransactionLockMap) ContainsResource(txId TransactionId, name ResourceName) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.data[txId]; !ok {
		m.data[txId] = make(map[uint64][]LockType)
		m.data[txId][name.hashValue] = make([]LockType, 0)
		return false
	}
	if _, ok := m.data[txId][name.hashValue]; !ok {
		m.data[txId][name.hashValue] = make([]LockType, 0)
		return false
	}
	return len(m.data[txId][name.hashValue]) > 0
}

func (m *TransactionLockMap) DeleteResource(txId TransactionId, name ResourceName) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.data[txId]; !ok {
		return
	}
	delete(m.data[txId], name.hashValue)
}
