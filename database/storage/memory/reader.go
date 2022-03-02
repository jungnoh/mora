package memory

import "github.com/jungnoh/mora/page"

type MemoryReader struct {
	pg       *memoryPage
	unlockFn UnlockFunc
}

func newMemoryReader(txId uint64, ptr *memoryPage) MemoryReader {
	unlock := ptr.lockS(txId)
	r := MemoryReader{
		pg:       ptr,
		unlockFn: unlock,
	}
	return r
}

func (r *MemoryReader) Get() *page.Page {
	return r.pg.content
}

func (r *MemoryReader) Done() {
	r.unlockFn()
	r.unlockFn = func() {}
	r.pg = nil
}
