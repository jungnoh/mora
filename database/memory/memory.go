package memory

import (
	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/database/wal"
	"github.com/jungnoh/mora/page"
)

type Memory struct {
	Config *util.Config
	Lock   *util.LockSet

	Map        map[string]MemoryPage
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
	LastFlushedTxId wal.TxID

	PrevLL *MemoryPage
	NextLL *MemoryPage
}

func (m *Memory) Exists(key string) bool {
	_, ok := m.Map[key]
	return ok
}

func (m *Memory) Access(key string) *page.Page {
	page, ok := m.Map[key]
	if ok {
		return &page.Content
	} else {
		return nil
	}
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
