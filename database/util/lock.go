package util

import "sync"

type MutexMap struct {
	access sync.Mutex
	locks  map[string]*sync.Mutex
}

func (m *MutexMap) Get(key string) *sync.Mutex {
	m.access.Lock()
	defer m.access.Unlock()

	if _, ok := m.locks[key]; !ok {
		m.locks[key] = &sync.Mutex{}
	}
	return m.locks[key]
}

type RWMutexMap struct {
	access sync.Mutex
	locks  map[string]*sync.RWMutex
}

func NewRWMutexMap() RWMutexMap {
	return RWMutexMap{
		access: sync.Mutex{},
		locks:  make(map[string]*sync.RWMutex),
	}
}

func (m *RWMutexMap) Get(key string) *sync.RWMutex {
	m.access.Lock()
	defer m.access.Unlock()

	if _, ok := m.locks[key]; !ok {
		m.locks[key] = &sync.RWMutex{}
	}
	return m.locks[key]
}

type LockSet struct {
	Disk      RWMutexMap
	Log       RWMutexMap
	Memory    RWMutexMap
	MemoryMap sync.RWMutex
	MemoryLL  sync.RWMutex
	WAL       sync.Mutex
}
