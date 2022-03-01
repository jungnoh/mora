package memory

import (
	"sync"

	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/page"
	"github.com/rs/zerolog/log"
)

type EvictionFunc func(dirty bool, content *page.Page) (shouldEvict bool, err error)

type Memory struct {
	data pageMap

	evictCheckLock sync.Mutex
	evictRunning   bool
}

func (m *Memory) HasPage(set common.UniqueKeyable) bool {
	return m.data.Has(set)
}

func (m *Memory) StartWrite(txId uint64, set common.UniqueKeyable) MemoryWriter {
	page, _ := m.data.Get(set)
	return newMemoryWriter(txId, page)
}

func (m *Memory) ForceWrite(set common.UniqueKeyable, content *page.Page) (added bool, err error) {
	added, err = m.data.AddIfNew(set, content)
	return
}

func (m *Memory) Read(txId uint64, set common.UniqueKeyable) (reader MemoryReader, ok bool) {
	page, readOk := m.data.Get(set)
	if !readOk {
		ok = false
		return
	}
	reader = newMemoryReader(txId, page)
	ok = true
	return
}

func (m *Memory) Init(set page.CandleSet) {
	m.data.InitIfNew(set)
}

func (m *Memory) RangeForEviction(fn EvictionFunc) {
	m.evictCheckLock.Lock()
	if m.evictRunning {
		log.Info().Msg("Memory eviction is already running. Will not run")
		m.evictCheckLock.Unlock()
		return
	}
	m.evictRunning = true
	m.evictCheckLock.Unlock()
	defer func() {
		m.evictCheckLock.Lock()
		m.evictRunning = false
		m.evictCheckLock.Unlock()
	}()

	m.data.Range(func(pg *memoryPage) bool {
		unlock := pg.lockForFlush()
		defer unlock()
		shouldEvict, err := fn(pg.dirty, pg.content)

		if err != nil {
			log.Warn().Err(err).Msg("Eviction run failed!")
			return false
		}
		if shouldEvict {
			log.Debug().Str("key", pg.content.UniqueKey()).Msg("evicting")
			m.data.Delete(pg.content)
		}
		return true
	})
	log.Info().Msg("Memory eviction complete")
}
