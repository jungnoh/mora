package memory

import (
	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/page"
)

type Memory struct {
	data pageMap
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
