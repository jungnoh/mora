package memory

import (
	"github.com/pkg/errors"
)

func (m *Memory) SetAsClean(page *MemoryPage) {
	m.Lock.MemoryLL.Lock()
	defer m.Lock.MemoryLL.Unlock()

	if page.InLL {
		if page.Dirty {
			m.removeFromDirtyLL(page)
		} else {
			m.removeFromCleanLL(page)
		}
	}
	m.addToCleanLL(page)
}

func (m *Memory) SetAsDirty(page *MemoryPage) {
	m.Lock.MemoryLL.Lock()
	defer m.Lock.MemoryLL.Unlock()

	if page.InLL {
		if page.Dirty {
			m.removeFromDirtyLL(page)
		} else {
			m.removeFromCleanLL(page)
		}
	}
	m.addToDirtyLL(page)
}

func (m *Memory) addToDirtyLL(page *MemoryPage) {
	// MemoryLL, page lock must be aquired
	if page.InLL || !page.Dirty {
		panic(errors.New("trying to insert page into dirty LL that is in LL or not dirty"))
	}
	if m.DirtyStart != nil && m.DirtyStart.Key != page.Key {
		prevLock := m.Lock.Memory.Get(m.DirtyStart.Key)
		prevLock.Lock()
		defer prevLock.Unlock()

		m.DirtyStart.PrevLL = page
	}
	page.InLL = true
	page.PrevLL = nil
	page.NextLL = m.DirtyStart
	m.DirtyStart = page
	m.DirtyCount++
}

func (m *Memory) addToCleanLL(page *MemoryPage) {
	// MemoryLL, page lock must be aquired
	if page.InLL || page.Dirty {
		panic(errors.New("trying to insert page into clean LL that is in LL or not clean"))
	}
	if m.CleanStart != nil && m.CleanStart.Key != page.Key {
		prevLock := m.Lock.Memory.Get(m.CleanStart.Key)
		prevLock.Lock()
		defer prevLock.Unlock()

		m.CleanStart.PrevLL = page
	}
	page.PrevLL = nil
	page.NextLL = m.CleanStart
	m.CleanStart = page
	m.CleanCount++
}

func (m *Memory) removeFromDirtyLL(page *MemoryPage) {
	// MemoryLL, page lock must be aquired
	if !(page.InLL && page.Dirty) {
		panic(errors.New("trying to remove page not in dirty LL"))
	}
	if page.PrevLL != nil {
		prevLock := m.Lock.Memory.Get(page.PrevLL.Key)
		prevLock.Lock()
		defer prevLock.Unlock()
	}
	if page.NextLL != nil {
		nextLock := m.Lock.Memory.Get(page.NextLL.Key)
		nextLock.Lock()
		defer nextLock.Unlock()
	}

	if m.DirtyStart == page {
		m.DirtyStart = page.NextLL
	} else if page.PrevLL != nil {
		page.PrevLL.NextLL = page.NextLL
	}
	if m.DirtyEnd == page {
		m.DirtyEnd = page.PrevLL
	} else if page.NextLL != nil {
		page.NextLL.PrevLL = page.PrevLL
	}

	page.InLL = false
	page.NextLL = nil
	page.PrevLL = nil
	m.DirtyCount--
}

func (m *Memory) removeFromCleanLL(page *MemoryPage) {
	// MemoryLL, page lock must be aquired
	if page.PrevLL != nil {
		prevLock := m.Lock.Memory.Get(page.PrevLL.Key)
		prevLock.Lock()
		defer prevLock.Unlock()
	}
	if page.NextLL != nil {
		nextLock := m.Lock.Memory.Get(page.NextLL.Key)
		nextLock.Lock()
		defer nextLock.Unlock()
	}

	if m.CleanStart == page {
		m.CleanStart = page.NextLL
	} else if page.PrevLL != nil {
		page.PrevLL.NextLL = page.NextLL
	}
	if m.CleanEnd == page {
		m.CleanEnd = page.PrevLL
	} else if page.NextLL != nil {
		page.NextLL.PrevLL = page.PrevLL
	}

	page.InLL = false
	page.NextLL = nil
	page.PrevLL = nil
	m.CleanCount--
}
