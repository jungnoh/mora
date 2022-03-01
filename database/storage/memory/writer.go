package memory

import "github.com/jungnoh/mora/page"

type MemoryWriter struct {
	original *memoryPage
	temp     *page.Page
	txId     uint64
	unlockFn UnlockFunc
}

func newMemoryWriter(txId uint64, ptr *memoryPage) MemoryWriter {
	unlock := ptr.lockX()
	w := MemoryWriter{
		original: ptr,
		txId:     txId,
		unlockFn: unlock,
	}
	if ptr.content != nil {
		copied := ptr.content.Copy()
		w.temp = &copied
	}
	return w
}

func (m *MemoryWriter) unlock() {
	m.unlockFn()
	m.unlockFn = func() {}
	m.original = nil
	m.temp = nil
}

func (m *MemoryWriter) WritableContent() *page.Page {
	return m.temp
}

func (m *MemoryWriter) Rollback() {
	m.unlock()
}

func (m *MemoryWriter) Commit() {
	m.original.content.Header = m.temp.Header
	m.original.content.Body = m.temp.Body
	if m.original.content.Header.LastTxId < m.txId {
		m.original.content.Header.LastTxId = m.txId
	}
	m.unlock()
}
