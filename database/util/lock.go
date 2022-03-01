package util

import (
	"sync"

	"github.com/rs/zerolog/log"
)

type MutexMap struct {
	access sync.Mutex
	locks  map[string]*sync.Mutex
}

func NewMutexMap() MutexMap {
	return MutexMap{
		access: sync.Mutex{},
		locks:  make(map[string]*sync.Mutex),
	}
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

// TODO: Remove MutexMap
type MutexSet struct {
	access sync.Mutex
	locks  map[string]*sync.Mutex
	name   string
}

func NewMutexSet(name string) *MutexSet {
	return &MutexSet{
		locks: make(map[string]*sync.Mutex),
		name:  name,
	}
}

func (s *MutexSet) get(key string) *sync.Mutex {
	s.access.Lock()
	defer s.access.Unlock()
	if _, ok := s.locks[key]; !ok {
		s.locks[key] = &sync.Mutex{}
	}
	return s.locks[key]
}

func (s *MutexSet) logLocking(key, message string) {
	log.Debug().Str("key", key).Str("set", s.name).Msg(message)
}

func (s *MutexSet) Lock(key string) func() {
	s.logLocking(key, "Trying to lock")
	lock := s.get(key)
	lock.Lock()
	s.logLocking(key, "Locked")

	unlocked := false
	return func() {
		if unlocked {
			return
		}
		s.logLocking(key, "Unlocking")
		lock.Unlock()
		unlocked = true
	}
}

// TODO: Remove RWMutexMap
type RWMutexSet struct {
	access sync.Mutex
	locks  map[string]*sync.RWMutex
	name   string
}

func NewRWMutexSet(name string) *RWMutexSet {
	return &RWMutexSet{
		locks: make(map[string]*sync.RWMutex),
		name:  name,
	}
}

func (s *RWMutexSet) get(key string) *sync.RWMutex {
	s.access.Lock()
	defer s.access.Unlock()
	if _, ok := s.locks[key]; !ok {
		s.locks[key] = &sync.RWMutex{}
	}
	return s.locks[key]
}

func (s *RWMutexSet) logLocking(mode, key, message string) {
	log.Debug().Str("key", key).Str("set", s.name).Str("mode", mode).Msg(message)
}

func (s *RWMutexSet) LockS(key string) func() {
	s.logLocking("S", key, "Trying to lock")
	lock := s.get(key)
	lock.RLock()
	s.logLocking("S", key, "Locked")

	unlocked := false
	return func() {
		if unlocked {
			return
		}
		s.logLocking("S", key, "Unlocking")
		lock.RUnlock()
		unlocked = true
	}
}

func (s *RWMutexSet) LockX(key string) func() {
	s.logLocking("X", key, "Trying to lock")
	lock := s.get(key)
	lock.Lock()
	s.logLocking("X", key, "Locked")

	unlocked := false
	return func() {
		if unlocked {
			return
		}
		s.logLocking("X", key, "Unlocking")
		lock.Unlock()
		unlocked = true
	}
}
