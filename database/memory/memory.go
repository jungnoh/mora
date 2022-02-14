package memory

import (
	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/page"
)

type Memory struct {
	Config *util.Config
	Lock   *util.LockSet

	Map        map[string]*MemoryPage
	DirtyCount int
	CleanCount int

	DirtyStart *MemoryPage
	DirtyEnd   *MemoryPage
	CleanStart *MemoryPage
	CleanEnd   *MemoryPage
}

type MemoryPage struct {
	Key             string
	InLL            bool
	Dirty           bool
	Content         page.Page
	LastFlushedTxId uint64

	PrevLL *MemoryPage
	NextLL *MemoryPage
}

func (m *Memory) Exists(key string) bool {
	m.Lock.MemoryMap.RLock()
	defer m.Lock.MemoryMap.RUnlock()
	_, ok := m.Map[key]
	return ok
}

func (m *Memory) AccessMemoryPage(key string) *MemoryPage {
	m.Lock.MemoryMap.RLock()
	page, ok := m.Map[key]
	m.Lock.MemoryMap.RUnlock()
	if ok {
		m.SetAsClean(page)
		return page
	} else {
		return nil
	}
}

func (m *Memory) Access(key string) *page.Page {
	result := m.AccessMemoryPage(key)
	if result == nil {
		return nil
	}
	return &result.Content
}

func (m *Memory) Evict() (evicted bool) {
	m.Lock.MemoryLL.Lock()
	defer m.Lock.MemoryLL.Unlock()

	if m.CleanEnd == nil {
		evicted = false
		return
	}
	evictingBlock := m.CleanEnd
	lastBlockLock := m.Lock.Memory.Get(evictingBlock.Key)
	lastBlockLock.Lock()
	defer lastBlockLock.Unlock()

	if m.CleanEnd.PrevLL != nil {
		l2BlockLock := m.Lock.Memory.Get(evictingBlock.PrevLL.Key)
		l2BlockLock.Lock()
		evictingBlock.PrevLL.NextLL = nil
		l2BlockLock.Unlock()
	}
	m.CleanEnd = evictingBlock.PrevLL
	m.CleanCount--
	delete(m.Map, evictingBlock.Key)
	evicted = true
	return
}

func (m *Memory) EvictNeeded() bool {
	m.Lock.MemoryLL.RLock()
	defer m.Lock.MemoryLL.RUnlock()
	return m.CleanCount > m.Config.MaxCleanBlocks
}

func (m *Memory) Insert(page page.Page) error {
	if m.EvictNeeded() {
		m.Evict()
	}

	pageKey := page.UniqueKey()
	if m.Exists(pageKey) {
		return nil
	}

	m.Lock.MemoryLL.Lock()
	defer m.Lock.MemoryLL.Unlock()

	m.Lock.MemoryMap.Lock()
	defer m.Lock.MemoryMap.Lock()
	m.Map[pageKey] = &MemoryPage{
		Key:             pageKey,
		Content:         page,
		LastFlushedTxId: page.Header.LastTxId,
	}
	m.addToCleanLL(m.Map[pageKey])

	return nil
}
