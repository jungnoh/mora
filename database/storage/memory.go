package storage

import "github.com/jungnoh/mora/page"

func (s *Storage) EvictMemory() {
	s.memory.RangeForEviction(func(dirty bool, content *page.Page) (shouldEvict bool, err error) {
		// TODO: Add implementation
		shouldEvict = true
		return
	})
}
